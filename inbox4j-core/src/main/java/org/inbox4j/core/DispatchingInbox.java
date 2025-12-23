/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.inbox4j.core;


import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.inbox4j.core.DelegationReferenceIssuer.IdVersion;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceededResult;
import org.inbox4j.core.InboxMessageChannel.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DispatchingInbox implements Inbox {

  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchingInbox.class);

  private static final Duration MINIMAL_RETENTION_PERIOD = Duration.ofHours(1);

  private final InboxMessageRepository repository;
  private final Dispatcher dispatcher;
  private final int maxConcurrency;
  private final ScheduledExecutorService internalExecutorService;
  private final Thread eventLoopThread;
  private final InstantSource instantSource;

  private final ExecutorService executorService;
  private final OtelPlugin otelPlugin;

  private final Deque<Recipient> recipientsToCheck = new LinkedList<>();
  private final Deque<RetryTriggered> retryRequests = new LinkedList<>();
  private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(100, true);
  private final DelegationReferenceIssuer delegationReferenceIssuer =
      new DelegationReferenceIssuer();
  private int parallelCount = 0;

  DispatchingInbox(
      InboxMessageRepository inboxMessageRepository,
      Dispatcher dispatcher,
      ExecutorService executorService,
      ScheduledExecutorService internalExecutorService,
      OtelPlugin otelPlugin,
      int maxConcurrency,
      Duration retentionPeriod,
      InstantSource instantSource) {
    this.repository = inboxMessageRepository;
    this.dispatcher = dispatcher;
    this.internalExecutorService = ensureInternalExecutorService(internalExecutorService);
    this.executorService = executorService;
    this.maxConcurrency = validateMaxConcurrency(maxConcurrency);
    this.instantSource = instantSource;
    this.otelPlugin = otelPlugin;

    this.eventLoopThread =
        new Thread(
            Thread.currentThread().getThreadGroup(), this::eventLoop, "inbox-message-event-loop");
    this.eventLoopThread.setDaemon(true);
    this.eventLoopThread.start();

    initializeRetention(retentionPeriod);
  }

  private static int validateMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be > 0");
    }
    LOGGER.debug("Configuring maxConcurrency = {}", maxConcurrency);
    return maxConcurrency;
  }

  private static void validateRetentionPeriod(Duration retentionPeriod) {
    if (retentionPeriod.compareTo(MINIMAL_RETENTION_PERIOD) < 0) {
      throw new IllegalArgumentException("timeRetention must be >= " + MINIMAL_RETENTION_PERIOD);
    }
  }

  private static ScheduledExecutorService ensureInternalExecutorService(
      ScheduledExecutorService executorService) {
    return executorService != null ? executorService : Executors.newSingleThreadScheduledExecutor();
  }

  private void initializeRetention(Duration timeRetention) {
    validateRetentionPeriod(timeRetention);
    Duration defaultCheckPeriod = Duration.ofDays(1);
    Duration checkPeriod =
        timeRetention.compareTo(defaultCheckPeriod) < 0 ? timeRetention : defaultCheckPeriod;
    internalExecutorService.scheduleAtFixedRate(
        this::cleanup, 0, checkPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public InboxMessage insert(MessageInsertionRequest request) {
    if (!dispatcher.isSupported(request.getChannelName())) {
      throw new IllegalArgumentException(
          "No channel with the name '" + request.getChannelName() + "' configured");
    }
    var inboxMessage = repository.insert(request);
    putEvent(new InboxMessageInserted(inboxMessage));
    return inboxMessage;
  }

  @Override
  public InboxMessage load(long id) {
    return repository.load(id);
  }

  @Override
  public void complete(DelegationReference delegationReference, boolean success) {
    IdVersion idVersion = delegationReferenceIssuer.dereference(delegationReference);
    InboxMessage message = repository.load(idVersion.id());
    if (message.getVersion() != idVersion.version()) {
      throw new StaleDataUpdateException(
          "InboxMessage{id="
              + message.getId()
              + "} version mismatch. Expected: "
              + idVersion.version()
              + ". Actual: "
              + message.getVersion());
    }

    Status status;
    if (success) {
      status = Status.COMPLETED;
    } else {
      status = Status.ERROR;
    }

    var maybeUpdatedMessage =
        tryUpdate(message).status(status).metadata(message.getMetadata()).retryAt(null).execute();

    putEvent(new DelegationCompleted(maybeUpdatedMessage));
  }

  @Override
  public void reset(long id) {
    repository.reset(id);
  }

  @Override
  public void remove(long id) {
    repository.delete(id);
  }

  private void eventLoop() {
    LOGGER.debug("Starting event loop");
    initializeEventLoop();
    try {
      while (!Thread.currentThread().isInterrupted()) {
        processNextEvent();

        while (shouldTryToFetchNextMessage()) {
          InboxMessage inboxMessage = tryToFetchNextMessage();

          if (inboxMessage != null) {
            markAsInProgressAndDispatch(inboxMessage);
          }
        }
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
    LOGGER.debug("Quiting dispatch loop");
  }

  private void initializeEventLoop() {
    scheduleRetries();
    loadWaitingRecipients();
    putEvent(new InitializationCompletedEvent());
  }

  private void processNextEvent() throws InterruptedException {
    Event event = events.take();
    LOGGER.debug("Taking event {} out of the event queue for processing", event);

    if (event instanceof InitializationCompletedEvent) {
      return;
    }

    var inboxMessageEvent = (InboxMessageEvent) event;

    if (inboxMessageEvent instanceof ProcessingTerminated) {
      parallelCount--;
      LOGGER.debug(
          "The event {} decrements the parallel count to the value {}", event, parallelCount);
    }

    if (inboxMessageEvent instanceof RetryTriggered retry) {
      retryRequests.add(retry);
    } else {
      recipientsToCheck.add(inboxMessageEvent.getRecipient());
    }
  }

  private boolean shouldTryToFetchNextMessage() {
    return parallelCount < maxConcurrency
        && (!retryRequests.isEmpty() || !recipientsToCheck.isEmpty());
  }

  private InboxMessage tryToFetchNextMessage() {
    if (!retryRequests.isEmpty()) {
      return retryRequests.removeFirst().message;
    } else if (!recipientsToCheck.isEmpty()) {
      return repository
          .findNextProcessingRelevant(recipientsToCheck.removeFirst().names())
          .filter(message -> message.getStatus().equals(Status.NEW))
          .orElse(null);
    } else {
      throw new IllegalStateException("Trying to fetch inbox message but no input available");
    }
  }

  private void putEvent(Event event) {
    LOGGER.debug("Putting event {} to the event queue", event);
    try {
      events.put(event);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void markAsInProgressAndDispatch(InboxMessage inboxMessage) {
    LOGGER
        .atDebug()
        .addArgument(inboxMessage::getId)
        .addArgument(inboxMessage::getRecipientNames)
        .log("Dispatching inbox message {id={}, recipientNames={}}");
    InboxMessage updatedMessage;
    try {
      updatedMessage =
          repository.update(inboxMessage, Status.IN_PROGRESS, inboxMessage.getMetadata(), null);
    } catch (StaleDataUpdateException staleDataUpdateException) {
      LOGGER
          .atDebug()
          .addArgument(inboxMessage::getId)
          .log(
              "Dispatching interrupted because inbox message {id={}} has been modified in the"
                  + " meantime");
      return;
    }
    parallelCount++;
    LOGGER.debug("Parallel count increased: {}", parallelCount);
    dispatch(updatedMessage);
  }

  private void loadWaitingRecipients() {
    List<Recipient> initialRecipientToCheck =
        repository.findAllProcessingRelevant().stream().map(InboxMessageView::recipient).toList();

    LOGGER.debug("Initialization: initial recipient names set: {}", initialRecipientToCheck);
    recipientsToCheck.addAll(initialRecipientToCheck);
  }

  private void scheduleRetries() {
    Instant currentTime = instantSource.instant();
    repository
        .findInboxMessagesForRetry()
        .forEach(
            messageView -> {
              var delay = Duration.between(currentTime, messageView.retryAt());
              if (delay.isNegative()) {
                delay = Duration.ZERO;
              }
              scheduleRetry(messageView.id(), delay);
            });
  }

  private void dispatch(InboxMessage message) {
    checkMessageIsInProgress(message);
    dispatcher
        .dispatch(message)
        .whenComplete(
            (processingResult, exception) -> {
              if (exception != null) {
                processingResult = new ProcessingFailedResult(message, exception);
              }
              LOGGER.debug(
                  "Processing of InboxMessage{id={}} finished with result {}",
                  message.getId(),
                  processingResult);

              InboxMessage updatedMessage;
              if (processingResult instanceof ProcessingSucceededResult succeededResult) {
                updatedMessage = handleProcessingSucceeded(succeededResult);
              } else if (processingResult instanceof ProcessingFailedResult failedResult) {
                updatedMessage = handleProcessingFailed(failedResult);
              } else if (processingResult instanceof RetryResult retryResult) {
                updatedMessage = handleRetryResult(retryResult);
              } else if (processingResult instanceof DelegateResult delegateResult) {
                updatedMessage = handleDelegateResult(delegateResult);
              } else {
                throw new UnsupportedOperationException(
                    "Result type" + processingResult.getClass().getName() + " not supported yet!");
              }
              putEvent(new ProcessingTerminated(updatedMessage));
            });
  }

  private void execute(Runnable runnable, String traceContext) {
    executorService.execute(() -> otelPlugin.getContextInstaller().install(traceContext, runnable));
  }

  private static void checkMessageIsInProgress(InboxMessage message) {
    if (!Status.IN_PROGRESS.equals(message.getStatus())) {
      throw new IllegalArgumentException(
          "InboxMessage{id="
              + message.getId()
              + "} can be dispatched because it has the status "
              + message.getStatus()
              + " and not the required status "
              + Status.IN_PROGRESS);
    }
  }

  private InboxMessage handleProcessingSucceeded(ProcessingSucceededResult succeeded) {
    return tryUpdate(succeeded.getInboxMessage())
        .status(Status.COMPLETED)
        .metadata(succeeded.getMetadata())
        .retryAt(null)
        .execute();
  }

  private InboxMessage handleProcessingFailed(ProcessingFailedResult failed) {
    LOGGER
        .atError()
        .setCause(failed.getError())
        .addArgument(failed.getInboxMessage()::getId)
        .log("InboxMessage{id={}} processing failed");

    return tryUpdate(failed.getInboxMessage())
        .status(Status.ERROR)
        .metadata(failed.getMetadata())
        .retryAt(null)
        .execute();
  }

  private InboxMessage handleRetryResult(RetryResult retryResult) {
    var currentInstant = instantSource.instant();
    var retryAt = currentInstant.plus(retryResult.getDelay());

    return tryUpdate(retryResult.getInboxMessage())
        .status(Status.RETRY)
        .metadata(retryResult.getMetadata())
        .retryAt(retryAt)
        .execute(updatedMessage -> scheduleRetry(updatedMessage.getId(), retryResult.getDelay()));
  }

  private UpdateStatusSpec tryUpdate(InboxMessage inboxMessage) {
    return new TryUpdateOperation(inboxMessage, repository);
  }

  private void scheduleRetry(long messageId, Duration delay) {
    internalExecutorService.schedule(
        () -> triggerRetry(messageId), delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void triggerRetry(long messageId) {
    InboxMessage message = repository.load(messageId);
    if (message.getStatus().equals(Status.RETRY)) {
      putEvent(new RetryTriggered(message));
    }
  }

  private InboxMessage handleDelegateResult(DelegateResult delegateResult) {
    return tryUpdate(delegateResult.getInboxMessage())
        .status(Status.DELEGATED)
        .metadata(delegateResult.getMetadata())
        .retryAt(null)
        .execute(
            updatedMessage ->
                execute(
                    () -> {
                      var delegationReference =
                          delegationReferenceIssuer.issueReference(updatedMessage);
                      delegateResult
                          .getDelegatingCallback()
                          .delegate(updatedMessage, delegationReference);
                    },
                    updatedMessage.getTraceContext()));
  }

  void cleanup() {
    throw new UnsupportedOperationException();
  }

  void stop() {
    eventLoopThread.interrupt();
    dispatcher.close();
    internalExecutorService.shutdown();

    try {
      eventLoopThread.join(5000);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private sealed interface Event {}

  private static final class InitializationCompletedEvent implements Event {}

  private sealed interface InboxMessageEvent extends Event {

    Recipient getRecipient();

    InboxMessage message();

    default String asString() {
      return getClass().getSimpleName()
          + "{inboxMessageId="
          + message().getId()
          + ", recipientNames="
          + message().getRecipientNames()
          + "}";
    }
  }

  private record InboxMessageInserted(InboxMessage message) implements InboxMessageEvent {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record ProcessingTerminated(InboxMessage message) implements InboxMessageEvent {

    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record DelegationCompleted(InboxMessage message) implements InboxMessageEvent {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record RetryTriggered(InboxMessage message) implements InboxMessageEvent {
    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }
}
