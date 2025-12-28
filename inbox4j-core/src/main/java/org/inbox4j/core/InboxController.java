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

import static java.lang.Thread.currentThread;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.inbox4j.core.ContinuationReferenceIssuer.IdVersion;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceededResult;
import org.inbox4j.core.InboxMessageChannel.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class InboxController implements Inbox {

  private static final Logger LOGGER = LoggerFactory.getLogger(InboxController.class);

  private final InboxMessageRepository repository;
  private final Dispatcher dispatcher;
  private final ContinuationExecutor continuationExecutor;
  private final RetentionPolicy retentionPolicy;
  private final int maxConcurrency;
  private final ScheduledExecutorService retryExecutor;
  private final ExecutorService eventLoopExecutor;
  private final InstantSource instantSource;

  private final Deque<Recipient> recipientsToCheck = new LinkedList<>();
  private final Deque<RetryTriggered> retryRequests = new LinkedList<>();
  private final BlockingQueue<Event> events = new LinkedBlockingQueue<>();
  private final AtomicBoolean started = new AtomicBoolean(false);
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private int parallelCount = 0;
  private Future<?> eventLoopTask;

  InboxController(
      InboxMessageRepository inboxMessageRepository,
      Dispatcher dispatcher,
      ContinuationExecutor continuationExecutor,
      RetentionPolicy retentionPolicy,
      ScheduledExecutorService retryExecutor,
      ExecutorService eventLoopExecutor,
      int maxConcurrency,
      InstantSource instantSource) {
    this.repository = inboxMessageRepository;
    this.dispatcher = dispatcher;
    this.continuationExecutor = continuationExecutor;
    this.retentionPolicy = retentionPolicy;
    this.retryExecutor = retryExecutor;
    this.maxConcurrency = validateMaxConcurrency(maxConcurrency);
    this.eventLoopExecutor = eventLoopExecutor;
    this.instantSource = instantSource;
  }

  private static int validateMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be > 0");
    }
    LOGGER.debug("Configuring maxConcurrency = {}", maxConcurrency);
    return maxConcurrency;
  }

  @Override
  public void start() {
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException("Inbox already started");
    }

    eventLoopTask = eventLoopExecutor.submit(this::eventLoop);
    retentionPolicy.apply();
  }

  @Override
  public InboxMessage submit(SubmitInboxMessageRequest submitRequest) {
    checkNotClosed();

    if (!dispatcher.isSupported(submitRequest.getChannelName())) {
      throw new IllegalArgumentException(
          "No channel with the name '" + submitRequest.getChannelName() + "' configured");
    }
    var inboxMessage = repository.insert(submitRequest);
    putEvent(new InboxMessageInserted(inboxMessage));
    return inboxMessage;
  }

  @Override
  public InboxMessage load(long id) {
    return repository.load(id);
  }

  @Override
  public void complete(ContinuationReference continuationReference, boolean success) {
    IdVersion idVersion = continuationExecutor.dereference(continuationReference);
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

    putEvent(new ContinuationCompleted(maybeUpdatedMessage));
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

    while (!currentThread().isInterrupted()) {
      try {
        processNextEvent();

        while (shouldTryToFetchNextMessage()) {
          InboxMessage inboxMessage = tryToFetchNextMessage();

          if (inboxMessage != null) {
            markAsInProgressAndDispatch(inboxMessage);
          }
        }
      } catch (InterruptedException interruptedException) {
        currentThread().interrupt();
      } catch (Exception exception) {
        LOGGER.error("Unexpected error during processing an event", exception);
      }
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
      recipientsToCheck.add(inboxMessageEvent.recipient());
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
      currentThread().interrupt();
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
              } else if (processingResult instanceof ContinuationResult continuationResult) {
                updatedMessage = handleContinuationResult(continuationResult);
              } else {
                throw new UnsupportedOperationException(
                    "Result type" + processingResult.getClass().getName() + " not supported yet!");
              }
              putEvent(new ProcessingTerminated(updatedMessage));
            });
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
    retryExecutor.schedule(() -> triggerRetry(messageId), delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void triggerRetry(long messageId) {
    InboxMessage message = repository.load(messageId);
    if (message.getStatus().equals(Status.RETRY)) {
      putEvent(new RetryTriggered(message));
    }
  }

  private InboxMessage handleContinuationResult(ContinuationResult continuationResult) {
    return tryUpdate(continuationResult.getInboxMessage())
        .status(Status.WAITING_FOR_CONTINUATION)
        .metadata(continuationResult.getMetadata())
        .retryAt(null)
        .execute(
            updatedMessage ->
                continuationExecutor.execute(updatedMessage, continuationResult.getContinuation()));
  }

  private void checkNotClosed() {
    if (closed.get()) {
      throw new RejectedExecutionException("Inbox closed");
    }
  }

  @Override
  public void close() {
    closed.set(true);
    dispatcher.shutdown();
    continuationExecutor.shutdown();
    retentionPolicy.shutdown();
    retryExecutor.shutdown();
    eventLoopExecutor.shutdown();
    eventLoopTask.cancel(true);

    Instant waitUntil = instantSource.instant().plusSeconds(30);

    try {
      for (Lifecycle lifecycle :
          List.of(
              dispatcher,
              continuationExecutor,
              retentionPolicy,
              new ExecutorAwareLifecycle<>(retryExecutor),
              new ExecutorAwareLifecycle<>(eventLoopExecutor))) {
        Duration duration = Duration.between(instantSource.instant(), waitUntil);
        lifecycle.awaitTermination(duration);
      }
    } catch (InterruptedException e) {
      currentThread().interrupt();
    }
  }

  private sealed interface Event {}

  private static final class InitializationCompletedEvent implements Event {}

  private sealed interface InboxMessageEvent extends Event {

    Recipient recipient();

    long id();

    default String asString() {
      return getClass().getSimpleName()
          + "{inboxMessageId="
          + id()
          + ", recipientNames="
          + recipient().names()
          + "}";
    }
  }

  private record InboxMessageInserted(long id, Recipient recipient) implements InboxMessageEvent {

    InboxMessageInserted(InboxMessage message) {
      this(message.getId(), new Recipient(message.getRecipientNames()));
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record ProcessingTerminated(long id, Recipient recipient) implements InboxMessageEvent {

    ProcessingTerminated(InboxMessage message) {
      this(message.getId(), new Recipient(message.getRecipientNames()));
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record ContinuationCompleted(long id, Recipient recipient) implements InboxMessageEvent {

    ContinuationCompleted(InboxMessage message) {
      this(message.getId(), new Recipient(message.getRecipientNames()));
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record RetryTriggered(InboxMessage message) implements InboxMessageEvent {

    @Override
    public long id() {
      return message.getId();
    }

    @Override
    public Recipient recipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }
}
