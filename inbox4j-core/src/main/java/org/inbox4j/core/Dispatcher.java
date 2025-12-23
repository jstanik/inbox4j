package org.inbox4j.core;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingResult;
import org.slf4j.Logger;

class Dispatcher implements Closeable {
  private static final Logger LOGGER = getLogger(Dispatcher.class);
  private final Map<String, InboxMessageChannel> channels;
  private final ExecutorService executor;
  private final OtelPlugin otelPlugin;

  Dispatcher(
      Collection<InboxMessageChannel> channels,
      ExecutorService executorService,
      OtelPlugin otelPlugin) {
    this.channels =
        Map.copyOf(channels.stream().collect(toMap(InboxMessageChannel::getName, identity())));
    this.executor = executorService;
    this.otelPlugin = otelPlugin;
  }

  CompletableFuture<ProcessingResult> dispatch(InboxMessage message) {
    return CompletableFuture.supplyAsync(
        () ->
            otelPlugin
                .getContextInstaller()
                .install(
                    message.getTraceContext(),
                    () -> {
                      var channel = resolveChannel(message);
                      return dispatchToChannel(message, channel);
                    }),
        executor);
  }

  boolean isSupported(String channelName) {
    return this.channels.containsKey(channelName);
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

  private ProcessingResult dispatchToChannel(InboxMessage message, InboxMessageChannel channel) {

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
    return processingResult;
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }
}
