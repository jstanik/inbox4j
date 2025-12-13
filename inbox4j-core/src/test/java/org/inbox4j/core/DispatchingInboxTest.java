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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceeded;
import org.inbox4j.core.InboxMessageChannel.Retry;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class DispatchingInboxTest extends AbstractDatabaseTest {

  private static final String CHANNEL = "channelName";

  @Test
  void inboxDispatching() {
    TestChannel channel = new TestChannel(CHANNEL);
    DispatchingInbox inbox =
        (DispatchingInbox) Inbox.builder(dataSource).addChannel(channel).build();

    InboxMessage recipient1Message1 = inbox.insert(inboxMessageData("recipient1"));
    InboxMessage recipient1Message2 = inbox.insert(inboxMessageData("recipient1"));
    InboxMessage recipient2Message1 = inbox.insert(inboxMessageData("recipient2"));
    InboxMessage recipient2Message2 = inbox.insert(inboxMessageData("recipient2"));

    var context = channel.awaitProcessing("recipient1");
    assertThat(context.message.getId()).isEqualTo(recipient1Message1.getId());
    context = channel.awaitProcessing("recipient2");
    assertThat(context.message.getId()).isEqualTo(recipient2Message1.getId());
    context = channel.awaitProcessing("recipient1");
    assertThat(context.message.getId()).isEqualTo(recipient1Message2.getId());
    context = channel.awaitProcessing("recipient2");
    assertThat(context.message.getId()).isEqualTo(recipient2Message2.getId());

    assertStatusReached(context.message.getId(), Status.COMPLETED, inbox, Duration.ofSeconds(10));

    inbox.stop();
  }

  @Test
  void inboxDispatchingWithRetry() {
    TestChannel channel = new TestChannel(CHANNEL);
    ScheduledExecutorService retryExecutor = mock(ScheduledExecutorService.class);
    MutableInstantSource instantSource =
        new MutableInstantSource(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    DispatchingInbox inbox =
        (DispatchingInbox)
            Inbox.builder(dataSource)
                .withRetryScheduler(retryExecutor)
                .withInstantSource(instantSource)
                .addChannel(channel)
                .build();

    InboxMessage message = inbox.insert(inboxMessageData("recipient1"));

    Duration retryDelay = Duration.ofMillis(100);
    var context = channel.awaitProcessing("recipient1", m -> new Retry(m, retryDelay));
    assertStatusReached(context.message.getId(), Status.RETRY, inbox, Duration.ofSeconds(10));
    var retryMessage = inbox.load(message.getId());
    assertThat(retryMessage.getRetryAt()).isEqualTo(instantSource.instant().plus(retryDelay));

    var runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(retryExecutor)
        .schedule(runnableCaptor.capture(), eq(retryDelay.toMillis()), eq(TimeUnit.MILLISECONDS));
    var retryAction = runnableCaptor.getValue();
    retryAction.run();
    context = channel.awaitProcessing("recipient1", ProcessingSucceeded::new);
    assertStatusReached(context.message.getId(), Status.COMPLETED, inbox, Duration.ofSeconds(10));
    inbox.stop();
  }

  @Test
  void inboxDispatchingRetriesFoundInDatabase() {
    MutableInstantSource instantSource =
        new MutableInstantSource(Instant.now().truncatedTo(ChronoUnit.MILLIS));

    InboxMessageRepository repo =
        new InboxMessageRepository(dataSource, instantSource, new OtelPlugin());
    var recipientName = "recipient-to-retry";
    var message = repo.insert(new MessageInsertionRequest(CHANNEL, new byte[0], recipientName));
    var retryAt = instantSource.instant().minus(100, ChronoUnit.MILLIS);
    repo.update(message, Status.RETRY, null, retryAt);

    TestChannel channel = new TestChannel(CHANNEL);
    DispatchingInbox inbox =
        (DispatchingInbox)
            Inbox.builder(dataSource).withInstantSource(instantSource).addChannel(channel).build();

    channel.awaitProcessing(recipientName);
    assertStatusReached(message.getId(), Status.COMPLETED, inbox, Duration.ofSeconds(10));

    inbox.stop();
  }

  @Test
  void inboxDispatchingWithOpenTelemetry() {
    TestChannel channel = new TestChannel(CHANNEL);

    DispatchingInbox inbox =
        (DispatchingInbox) Inbox.builder(dataSource).addChannel(channel).build();

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("inbox")
            .setParent(Context.root())
            .startSpan();

    String expectedTraceId;
    try (Scope ignore = span.makeCurrent()) {
      inbox.insert(inboxMessageData("recipient1"));
      expectedTraceId = span.getSpanContext().getTraceId();
    } finally {
      span.end();
    }

    var context = channel.awaitProcessing("recipient1");
    assertThat(context.traceId).isEqualTo(expectedTraceId);
    inbox.stop();
  }

  @Test
  void failedProcessingPutsMessageToErrorState() {
    FailingChannel channel = new FailingChannel(CHANNEL);
    DispatchingInbox inbox =
        (DispatchingInbox) Inbox.builder(dataSource).addChannel(channel).build();

    InboxMessage message = inbox.insert(inboxMessageData("recipient1"));

    assertStatusReached(message.getId(), Status.ERROR, inbox, Duration.ofSeconds(10));
    inbox.stop();
  }

  private static void assertStatusReached(
      long id, Status expectedStatus, DispatchingInbox inbox, Duration timeout) {
    long tryUntil = Instant.now().toEpochMilli() + timeout.toMillis();

    while (tryUntil > Instant.now().toEpochMilli()) {
      var message = inbox.load(id);
      if (expectedStatus.equals(message.getStatus())) {
        return;
      }
    }

    fail("InboxMessage{id=" + id + "} hasn't reached the expected status " + expectedStatus);
  }

  private static MessageInsertionRequest inboxMessageData(String recipientName) {
    return new MessageInsertionRequest(CHANNEL, new byte[0], recipientName);
  }

  private static class TestChannel implements InboxMessageChannel {

    private final String name;
    private final Map<String, CountDownLatch> awaitLatchMap = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> processLatchMap = new ConcurrentHashMap<>();
    private final Map<String, InboxMessage> lastProcessedMessages = new ConcurrentHashMap<>();
    private final Map<String, String> lastTraceIds = new ConcurrentHashMap<>();
    private final Map<String, Function<InboxMessage, ProcessingResult>> resultProviders =
        new ConcurrentHashMap<>();

    public TestChannel(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ProcessingResult processMessage(InboxMessage message, ProcessingContext context) {
      String recipientName = message.getRecipientNames().stream().findFirst().orElseThrow();
      lastProcessedMessages.put(recipientName, message);
      lastTraceIds.put(recipientName, Span.current().getSpanContext().getTraceId());
      awaitLatchMap.computeIfAbsent(recipientName, k -> new CountDownLatch(1)).countDown();
      try {
        processLatchMap
            .computeIfAbsent(recipientName, k -> new CountDownLatch(1))
            .await(60, TimeUnit.SECONDS);
        processLatchMap.put(recipientName, new CountDownLatch(1));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      var resultProvider =
          resultProviders.computeIfAbsent(recipientName, key -> ProcessingSucceeded::new);
      return resultProvider.apply(message);
    }

    InboxMessageContext awaitProcessing(String recipientName) {
      return awaitProcessing(recipientName, ProcessingSucceeded::new);
    }

    InboxMessageContext awaitProcessing(
        String recipientName, Function<InboxMessage, ProcessingResult> resultProvider) {
      InboxMessage message;
      String traceId;
      try {
        var latch = awaitLatchMap.computeIfAbsent(recipientName, k -> new CountDownLatch(1));
        latch.await(60, TimeUnit.SECONDS);
        awaitLatchMap.put(recipientName, new CountDownLatch(1));
        message = lastProcessedMessages.remove(recipientName);
        traceId = lastTraceIds.remove(recipientName);
        resultProviders.put(recipientName, resultProvider);
        processLatchMap.computeIfAbsent(recipientName, k -> new CountDownLatch(1)).countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }

      return new InboxMessageContext(message, traceId);
    }
  }

  private record InboxMessageContext(InboxMessage message, String traceId) {}

  private record FailingChannel(String name) implements InboxMessageChannel {

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ProcessingFailed processMessage(InboxMessage message, ProcessingContext context) {
      return new ProcessingFailed(message, new RuntimeException("Processing failed"));
    }
  }
}
