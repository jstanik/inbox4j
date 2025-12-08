package org.inbox4j;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.inbox4j.InboxMessage.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ProcessingInbox implements Inbox {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProcessingInbox.class);

  private final InboxMessageRepository repository;
  private final Map<String, InboxMessageChannel> channels;
  private final int maxConcurrency;
  private final ExecutorService executorService;
  private final Thread processingLoopThread;

  private final List<Names> namesToCheck = new LinkedList<>();
  private final BlockingQueue<Event> events = new ArrayBlockingQueue<>(100, true);
  private int parallelCount = 0;

  ProcessingInbox(
      InboxMessageRepository inboxMessageRepository,
      Collection<InboxMessageChannel> channels,
      ExecutorService executorService,
      int maxConcurrency
  ) {
    this.repository = inboxMessageRepository;
    this.channels = Map.copyOf(
        channels.stream().collect(toMap(InboxMessageChannel::getName, identity())));
    this.maxConcurrency = maxConcurrency;
    this.executorService = executorService;

    this.processingLoopThread = new Thread(
        Thread.currentThread().getThreadGroup(),
        this::processingLoop,
        "inbox-message-processing-loop"
    );
    this.processingLoopThread.setDaemon(true);
    this.processingLoopThread.start();
  }

  @Override
  public InboxMessage insert(InboxMessageData data) {
    if (channels.get(data.getChannelName()) == null) {
      throw new IllegalArgumentException(
          "No channel with the name '" + data.getChannelName() + "' configured");
    }
    var inboxMessage = repository.insert(data);
    events.add(new InboxMessageInserted(inboxMessage));
    return inboxMessage;
  }

  private void processingLoop() {
    initNamesToCheck();
    try {
      while (!Thread.currentThread().isInterrupted()) {
        Event event = events.take();

        if (event instanceof ProcessingTerminated) {
          parallelCount--;
          LOGGER.debug("Parallel count decreased: {}", parallelCount);
        }

        namesToCheck.add(event.getNames());

        while (parallelCount <= maxConcurrency && !namesToCheck.isEmpty()) {
          var inboxMessage = repository.findNextProcessingRelevant(namesToCheck.remove(0).names())
              .orElse(null);
          if (inboxMessage == null) {
            continue;
          }

          startProcessing(inboxMessage);
        }
      }
    } catch (InterruptedException interruptedException) {
      Thread.currentThread().interrupt();
    }
  }

  private void startProcessing(InboxMessage inboxMessage) {
    try {
      inboxMessage = repository.update(inboxMessage, Status.IN_PROGRESS,
          inboxMessage.getMetadata());
      parallelCount++;
      LOGGER.debug("Parallel count increased: {}", parallelCount);
      dispatch(inboxMessage);
    } catch (StaleDataUpdateException staleDataUpdateException) {
      LOGGER.debug("Failed to change status of the message {} to {} due to stale data.",
          inboxMessage.getId(), Status.IN_PROGRESS);
    }
  }

  private void initNamesToCheck() {
    List<Names> initialNamesToCheck = repository.findAllProcessingRelevant()
        .stream().map(InboxMessageReferences::targetNames).toList();

    LOGGER.debug("Initialization: initial target names set: {}", initialNamesToCheck);
    namesToCheck.addAll(initialNamesToCheck);
  }

  private void dispatch(InboxMessage message) {
    if (!Status.IN_PROGRESS.equals(message.getStatus())) {
      throw new IllegalArgumentException(
          "InboxMessage{id=" + message.getId() + "} can be dispatched because it has the status "
              + message.getStatus() + " and not the required status " + Status.IN_PROGRESS);
    }

    var channel = channels.get(message.getChannelName());
    if (channel == null) {
      throw new IllegalStateException(
          "InboxMessage{id=" + message.getId() + "} refers to non existing channel: "
              + message.getChannelName());
    }

    CompletableFuture<Void> completableFuture = new CompletableFuture<>();
    completableFuture.whenComplete((result, error) -> {
      InboxMessage updateMessage;
      if (error != null) {
        LOGGER.error("Channel {} failed to process the InboxMessage{id={}}", channel.getName(),
            message.getId(), error);
        updateMessage = repository.update(message, Status.ERROR, message.getMetadata());
      } else {
        LOGGER.debug("Channel {} processed the InboxMessage{id={}} successfully.",
            channel.getName(), message.getId());
        updateMessage = repository.update(message, Status.COMPLETED, message.getMetadata());
      }

      events.add(new ProcessingInbox.ProcessingTerminated(updateMessage));
    });

    executorService.execute(() -> {
      try {
        channel.processMessage(message);
        completableFuture.complete(null);
      } catch (Exception exception) {
        completableFuture.completeExceptionally(exception);
      }
    });
  }

  void stop() {
    processingLoopThread.interrupt();
    executorService.shutdown();
  }

  private sealed interface Event {

    Names getNames();
  }

  private record InboxMessageInserted(InboxMessage message) implements Event {

    @Override
    public Names getNames() {
      return new Names(message.getTargetNames());
    }
  }

  private record ProcessingTerminated(InboxMessage message) implements Event {

    @Override
    public Names getNames() {
      return new Names(message.getTargetNames());
    }
  }
}
