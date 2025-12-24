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
import static org.slf4j.LoggerFactory.getLogger;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingResult;
import org.slf4j.Logger;

class Dispatcher extends AbstractExecutorAwareLifecycle<ExecutorService> {
  private static final Logger LOGGER = getLogger(Dispatcher.class);
  private final Map<String, InboxMessageChannel> channels;
  private final OtelPlugin otelPlugin;

  Dispatcher(
      Collection<InboxMessageChannel> channels,
      ExecutorService executorService,
      OtelPlugin otelPlugin) {
    super(executorService);
    this.channels =
        Map.copyOf(channels.stream().collect(toMap(InboxMessageChannel::getName, identity())));
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
  public void shutdown() {
    executor.shutdown();
  }

  @Override
  public void awaitTermination(Duration timeout) {
    try {
      if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      executor.shutdownNow();
    }
  }
}
