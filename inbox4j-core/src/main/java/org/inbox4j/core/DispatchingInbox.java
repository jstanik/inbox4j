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

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.inbox4j.core.DelegationReferenceIssuer.IdVersion;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceededResult;
import org.inbox4j.core.InboxMessageChannel.RetryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DispatchingInbox implements Inbox {

  private static final Logger LOGGER = LoggerFactory.getLogger(DispatchingInbox.class);

  private final InboxMessageRepository repository;
  private final Map<String, InboxMessageChannel> channels;
  private final int maxConcurrency;
  private final OtelPlugin otelPlugin;
  private final ExecutorService executorService;
  private final ScheduledExecutorService retryScheduler;
  private final Thread eventLoopThread;
  private final InstantSource instantSource;

  private final Deque<Recipient> recipientsToCheck = new LinkedList<>();
  private final Deque<RetryFired> retryRequests = new LinkedList<>();
  private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(100, true);
  private final DelegationReferenceIssuer delegationReferenceIssuer =
      new DelegationReferenceIssuer();
  private int parallelCount = 0;

  DispatchingInbox(
      InboxMessageRepository inboxMessageRepository,
      Collection<InboxMessageChannel> channels,
      ExecutorService executorService,
      ScheduledExecutorService retryScheduler,
      OtelPlugin otelPlugin,
      int maxConcurrency,
      InstantSource instantSource) {
    this.repository = inboxMessageRepository;
    this.channels =
        Map.copyOf(channels.stream().collect(toMap(InboxMessageChannel::getName, identity())));
    this.executorService = executorService;
    this.retryScheduler = retryScheduler;
    this.otelPlugin = otelPlugin;
    this.maxConcurrency = validateMaxConcurrency(maxConcurrency);
    this.instantSource = instantSource;

    this.eventLoopThread =
        new Thread(
            Thread.currentThread().getThreadGroup(), this::eventLoop, "inbox-message-event-loop");
    this.eventLoopThread.setDaemon(true);
    this.eventLoopThread.start();
  }

  private static int validateMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be > 0");
    }
    LOGGER.debug("Configuring maxConcurrency = {}", maxConcurrency);
    return maxConcurrency;
  }

  @Override
  public InboxMessage insert(MessageInsertionRequest request) {
    if (channels.get(request.getChannelName()) == null) {
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

  private void eventLoop() {
    LOGGER.debug("Starting event loop");

    scheduleRetries();
    loadWaitingRecipients();
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

  private void processNextEvent() throws InterruptedException {
    Event event = events.take();
    LOGGER.debug("Taking event {} out of the event queue for processing", event);

    if (event instanceof ProcessingTerminated) {
      parallelCount--;
      LOGGER.debug(
          "The event {} decrements the parallel count to the value {}", event, parallelCount);
    }

    if (event instanceof RetryFired retry) {
      retryRequests.add(retry);
    } else {
      recipientsToCheck.add(event.getRecipient());
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
    var action = createDispatchingAction(message);
    execute(action, message.getTraceContext());
  }

  private void execute(Runnable runnable, String traceContext) {
    executorService.execute(() -> otelPlugin.getContextInstaller().install(traceContext, runnable));
  }

  private Runnable createDispatchingAction(InboxMessage message) {
    var channel = resolveChannel(message);
    return () -> {
      LOGGER.debug(
          "Inbox message {id={}, recipientNames={}} dispatched to the channel '{}'",
          message.getId(),
          message.getRecipientNames(),
          channel.getName());
      ProcessingResult processingResult;
      try {
        processingResult = channel.processMessage(message);
      } catch (Exception exception) {
        processingResult = new ProcessingFailedResult(message, exception);
      }
      LOGGER.debug(
          "Channel '{}' finished processing and returned result {}",
          channel.getName(),
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
    };
  }

  private InboxMessageChannel resolveChannel(InboxMessage message) {
    var channel = channels.get(message.getChannelName());
    if (channel == null) {
      throw new IllegalStateException(
          "InboxMessage{id="
              + message.getId()
              + "} refers to non existing channel: "
              + message.getChannelName());
    }
    return channel;
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
    return new UpdateBuilder(inboxMessage);
  }

  private void scheduleRetry(long messageId, Duration delay) {
    retryScheduler.schedule(() -> fireRetry(messageId), delay.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void fireRetry(long messageId) {
    InboxMessage message = repository.load(messageId);
    if (message.getStatus().equals(Status.RETRY)) {
      putEvent(new RetryFired(message));
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

  void stop() {
    eventLoopThread.interrupt();
    executorService.shutdown();
    retryScheduler.shutdown();

    try {
      eventLoopThread.join(5000);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private sealed interface Event {

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

  private record InboxMessageInserted(InboxMessage message) implements Event {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record ProcessingTerminated(InboxMessage message) implements Event {

    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record DelegationCompleted(InboxMessage message) implements Event {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private record RetryFired(InboxMessage message) implements Event {
    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }

    @Override
    public String toString() {
      return asString();
    }
  }

  private interface UpdateStatusSpec {
    UpdateMetadataSpec status(Status status);
  }

  private interface UpdateMetadataSpec {
    UpdateRetryAtSpec metadata(byte[] metadata);
  }

  private interface UpdateExecutor {
    InboxMessage execute();

    InboxMessage execute(Consumer<InboxMessage> postUpdateAction);
  }

  private interface UpdateRetryAtSpec extends UpdateExecutor {
    UpdateExecutor retryAt(Instant retryAt);
  }

  private class UpdateBuilder implements UpdateStatusSpec, UpdateMetadataSpec, UpdateRetryAtSpec {

    private InboxMessage inboxMessage;
    private Status status;
    private byte[] metadata;
    private Instant retryAt;

    UpdateBuilder(InboxMessage inboxMessage) {
      this.inboxMessage = inboxMessage;
    }

    @Override
    public UpdateRetryAtSpec metadata(byte[] metadata) {
      this.metadata = metadata;
      return this;
    }

    @Override
    public UpdateExecutor retryAt(Instant retryAt) {
      this.retryAt = retryAt;
      return this;
    }

    @Override
    public UpdateMetadataSpec status(Status status) {
      this.status = status;
      return this;
    }

    @Override
    public InboxMessage execute() {
      return execute(m -> {});
    }

    @Override
    public InboxMessage execute(Consumer<InboxMessage> postUpdate) {
      try {
        var updatedMessage = repository.update(inboxMessage, status, metadata, retryAt);
        postUpdate.accept(updatedMessage);
        return updatedMessage;
      } catch (Exception exception) {
        LOGGER.error("Update of InboxMessage{id={}} failed", inboxMessage.getId(), exception);
        return inboxMessage;
      }
    }
  }
}
