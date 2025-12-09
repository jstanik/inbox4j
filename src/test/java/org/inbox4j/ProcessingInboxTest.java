package org.inbox4j;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.inbox4j.InboxMessage.Status;
import org.junit.jupiter.api.Test;

class ProcessingInboxTest extends AbstractDatabaseTest {

  private static final String CHANNEL = "channelName";

  @Test
  void inboxProcessing() {
    TestChannel channel = new TestChannel(CHANNEL);
    ProcessingInbox inbox = (ProcessingInbox) Inbox.create(dataSource).addChannel(channel)
        .build();

    InboxMessage target1Message1 = inbox.insert(inboxMessageData("target1"));
    InboxMessage target1Message2 = inbox.insert(inboxMessageData("target1"));
    InboxMessage target2Message1 = inbox.insert(inboxMessageData("target2"));
    InboxMessage target2Message2 = inbox.insert(inboxMessageData("target2"));

    var context = channel.awaitProcessing("target1");
    assertThat(context.message.getId()).isEqualTo(target1Message1.getId());
    context = channel.awaitProcessing("target2");
    assertThat(context.message.getId()).isEqualTo(target2Message1.getId());
    context = channel.awaitProcessing("target1");
    assertThat(context.message.getId()).isEqualTo(target1Message2.getId());
    context = channel.awaitProcessing("target2");
    assertThat(context.message.getId()).isEqualTo(target2Message2.getId());

    assertStatusReached(context.message.getId(), Status.COMPLETED, inbox, Duration.ofSeconds(10));

    inbox.stop();
  }

  @Test
  void inboxProcessingWithOtel() {
    TestChannel channel = new TestChannel(CHANNEL);

    ProcessingInbox inbox = (ProcessingInbox) Inbox.create(dataSource).addChannel(channel)
        .build();

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("inbox")
            .setParent(Context.root())
            .startSpan();

    String expectedTraceId;
    try (Scope ignore = span.makeCurrent()) {
      inbox.insert(inboxMessageData("target1"));
      expectedTraceId = span.getSpanContext().getTraceId();
    } finally {
      span.end();
    }

    var context = channel.awaitProcessing("target1");
    assertThat(context.traceId).isEqualTo(expectedTraceId);
    inbox.stop();
  }

  @Test
  void failedProcessingPutsMessageToErrorState() {
    FailingChannel channel = new FailingChannel(CHANNEL);
    ProcessingInbox inbox = (ProcessingInbox) Inbox.create(dataSource).addChannel(channel)
        .build();

    InboxMessage message = inbox.insert(inboxMessageData("target1"));

    assertStatusReached(message.getId(), Status.ERROR, inbox, Duration.ofSeconds(10));
    inbox.stop();
  }

  private static void assertStatusReached(long id, Status expectedStatus, ProcessingInbox inbox,
      Duration timeout) {
    long tryUntil = Instant.now().toEpochMilli() + timeout.toMillis();

    while (tryUntil > Instant.now().toEpochMilli()) {
      var message = inbox.load(id);
      if (expectedStatus.equals(message.getStatus())) {
        return;
      }
    }

    fail("InboxMessage{id=" + id + "} hasn't reached the expected status " + expectedStatus);
  }

  private static InboxMessageData inboxMessageData(String targetName) {
    return new InboxMessageData(CHANNEL, new byte[0], targetName);
  }

  private static class TestChannel implements InboxMessageChannel {

    private final String name;
    private Map<String, CountDownLatch> awaitLatchMap = new ConcurrentHashMap<>();
    private Map<String, CountDownLatch> processLatchMap = new ConcurrentHashMap<>();
    private Map<String, InboxMessage> lastProcessedMessages = new ConcurrentHashMap<>();
    private Map<String, String> lastTraceIds = new ConcurrentHashMap<>();

    public TestChannel(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void processMessage(InboxMessage message) {
      String targetName = message.getTargetNames().stream().findFirst().orElseThrow();
      lastProcessedMessages.put(targetName, message);
      lastTraceIds.put(targetName, Span.current().getSpanContext().getTraceId());
      awaitLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1)).countDown();
      try {
        processLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1))
            .await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    InboxMessageContext awaitProcessing(String targetName) {
      InboxMessage message;
      String traceId;
      try {
        var latch = awaitLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1));
        latch.await(60, TimeUnit.SECONDS);
        message = lastProcessedMessages.remove(targetName);
        traceId = lastTraceIds.remove(targetName);
        processLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1)).countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      awaitLatchMap.put(targetName, new CountDownLatch(1));

      return new InboxMessageContext(message, traceId);
    }
  }

  private record InboxMessageContext(InboxMessage message, String traceId) {

  }

  private record FailingChannel(String name) implements InboxMessageChannel {

    @Override
    public String getName() {
      return name;
    }

    @Override
    public void processMessage(InboxMessage message) {
      throw new RuntimeException("Processing failed");
    }
  }

}