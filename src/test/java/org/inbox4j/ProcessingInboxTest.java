package org.inbox4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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

    var processedMessage = channel.awaitProcessing("target1");
    assertThat(processedMessage.getId()).isEqualTo(target1Message1.getId());
    processedMessage = channel.awaitProcessing("target2");
    assertThat(processedMessage.getId()).isEqualTo(target2Message1.getId());
    processedMessage = channel.awaitProcessing("target1");
    assertThat(processedMessage.getId()).isEqualTo(target1Message2.getId());
    processedMessage = channel.awaitProcessing("target2");
    assertThat(processedMessage.getId()).isEqualTo(target2Message2.getId());

    inbox.stop();
  }

  private static InboxMessageData inboxMessageData(String targetName) {
    return new InboxMessageData(CHANNEL, new byte[0], targetName);
  }

  private static class TestChannel implements InboxMessageChannel {

    private final String name;
    private Map<String, CountDownLatch> awaitLatchMap = new ConcurrentHashMap<>();
    private Map<String, CountDownLatch> processLatchMap = new ConcurrentHashMap<>();
    private Map<String, InboxMessage> lastProcessedMessages = new ConcurrentHashMap<>();

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
      awaitLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1)).countDown();
      try {
        processLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1)).await(60, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    InboxMessage awaitProcessing(String targetName) {
      InboxMessage message;
      try {
        var latch = awaitLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1));
        latch.await(60, TimeUnit.SECONDS);
        message = lastProcessedMessages.remove(targetName);
        processLatchMap.computeIfAbsent(targetName, k -> new CountDownLatch(1)).countDown();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      awaitLatchMap.put(targetName, new CountDownLatch(1));

      return message;
    }

    InboxMessage getLastProcessedMessage(String targetName) {
      return lastProcessedMessages.get(targetName);
    }
  }

}