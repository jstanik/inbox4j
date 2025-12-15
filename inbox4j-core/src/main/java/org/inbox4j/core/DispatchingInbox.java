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
import org.inbox4j.core.DelegationReferenceIssuer.IdVersion;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailed;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceeded;
import org.inbox4j.core.InboxMessageChannel.Retry;
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
  private final Thread dispatchLoopThread;
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

    this.dispatchLoopThread =
        new Thread(
            Thread.currentThread().getThreadGroup(),
            this::dispatchLoop,
            "inbox-message-dispatch-loop");
    this.dispatchLoopThread.setDaemon(true);
    this.dispatchLoopThread.start();
  }

  private static int validateMaxConcurrency(int maxConcurrency) {
    if (maxConcurrency <= 0) {
      throw new IllegalArgumentException("maxConcurrency must be > 0");
    }
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
    var updatedMessage = repository.update(message, status, message.getMetadata(), null);
    putEvent(new DelegationCompleted(updatedMessage));
  }

  private void dispatchLoop() {
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
  }

  private void processNextEvent() throws InterruptedException {
    LOGGER.atDebug().addArgument(events::size).log("Event Queue size: {}");
    Event event = events.take();
    LOGGER.debug("Event received: {}", event);

    if (event instanceof ProcessingTerminated) {
      parallelCount--;
      LOGGER.debug("Parallel count decreased: {}", parallelCount);
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
      throw new IllegalStateException("Nothing to fetch!");
    }
  }

  private void putEvent(Event event) {
    try {
      events.put(event);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void markAsInProgressAndDispatch(InboxMessage inboxMessage) {
    try {
      inboxMessage =
          repository.update(inboxMessage, Status.IN_PROGRESS, inboxMessage.getMetadata(), null);
      parallelCount++;
      LOGGER.debug("Parallel count increased: {}", parallelCount);
      dispatch(inboxMessage);
    } catch (StaleDataUpdateException staleDataUpdateException) {
      LOGGER.debug(
          "Failed to change status of the message {} to {} due to stale data.",
          inboxMessage.getId(),
          Status.IN_PROGRESS);
    }
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
          "Dispatching InboxMessage{id={}} to the channel: {}", message.getId(), channel.getName());
      var processingResult = channel.processMessage(message);
      LOGGER.debug(
          "Channel {} finished processing the InboxMessage{id={}} with the result: {}",
          channel.getName(),
          message.getId(),
          processingResult.getClass().getSimpleName());
      InboxMessage updatedMessage;

      if (processingResult instanceof ProcessingSucceeded succeeded) {
        updatedMessage = handleProcessingSucceeded(succeeded);
      } else if (processingResult instanceof ProcessingFailed failed) {
        updatedMessage = handleProcessingFailed(failed);
      } else if (processingResult instanceof Retry retry) {
        updatedMessage = handleRetryRequested(retry);
      } else if (processingResult instanceof Delegate delegate) {
        updatedMessage = handleDelegateRequested(delegate);
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

  private InboxMessage handleProcessingSucceeded(ProcessingSucceeded succeeded) {
    return repository.update(
        succeeded.inboxMessage, Status.COMPLETED, succeeded.getMetadata(), null);
  }

  private InboxMessage handleProcessingFailed(ProcessingFailed failed) {
    return repository.update(failed.inboxMessage, Status.ERROR, failed.getMetadata(), null);
  }

  private InboxMessage handleRetryRequested(Retry retry) {
    var currentInstant = instantSource.instant();
    var retryAt = currentInstant.plus(retry.getDelay());

    var updatedMessage =
        repository.update(retry.inboxMessage, Status.RETRY, retry.getMetadata(), retryAt);
    scheduleRetry(updatedMessage.getId(), retry.getDelay());
    return updatedMessage;
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

  private InboxMessage handleDelegateRequested(Delegate delegate) {
    var updatedMessage =
        repository.update(delegate.inboxMessage, Status.DELEGATED, delegate.getMetadata(), null);

    execute(
        () -> {
          var delegationReference = delegationReferenceIssuer.issueReference(updatedMessage);
          delegate.getDelegatingCallback().delegate(updatedMessage, delegationReference);
        },
        updatedMessage.getTraceContext());

    return updatedMessage;
  }

  void stop() {
    dispatchLoopThread.interrupt();
    executorService.shutdown();
    retryScheduler.shutdown();

    try {
      dispatchLoopThread.join(5000);
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private sealed interface Event {

    Recipient getRecipient();
  }

  private record InboxMessageInserted(InboxMessage message) implements Event {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }
  }

  private record ProcessingTerminated(InboxMessage message) implements Event {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }
  }

  private record DelegationCompleted(InboxMessage message) implements Event {

    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }
  }

  private record RetryFired(InboxMessage message) implements Event {
    @Override
    public Recipient getRecipient() {
      return new Recipient(message.getRecipientNames());
    }
  }
}
