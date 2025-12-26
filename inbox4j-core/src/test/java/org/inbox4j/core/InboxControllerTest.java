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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceededResult;
import org.inbox4j.core.InboxMessageChannel.RetryResult;
import org.inbox4j.core.InboxMessageRepository.Configuration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class InboxControllerTest extends AbstractDatabaseTest {

  private static final String CHANNEL = "channelName";

  private InboxController cut;

  @AfterEach
  void tearDown() {
    if (cut != null) {
      cut.close();
    }
  }

  @Test
  void inboxDispatching() {
    TestChannel channel = new TestChannel(CHANNEL);
    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();

    InboxMessage recipient1Message1 = cut.submit(inboxMessageData("recipient1"));
    InboxMessage recipient1Message2 = cut.submit(inboxMessageData("recipient1"));
    InboxMessage recipient2Message1 = cut.submit(inboxMessageData("recipient2"));
    InboxMessage recipient2Message2 = cut.submit(inboxMessageData("recipient2"));

    var context = channel.awaitProcessing("recipient1");
    assertThat(context.message.getId()).isEqualTo(recipient1Message1.getId());
    context = channel.awaitProcessing("recipient2");
    assertThat(context.message.getId()).isEqualTo(recipient2Message1.getId());
    context = channel.awaitProcessing("recipient1");
    assertThat(context.message.getId()).isEqualTo(recipient1Message2.getId());
    context = channel.awaitProcessing("recipient2");
    assertThat(context.message.getId()).isEqualTo(recipient2Message2.getId());

    assertStatusReached(context.message.getId(), Status.COMPLETED, cut, Duration.ofSeconds(10));
  }

  @Test
  void inboxDispatchingWithRetry() {
    TestChannel channel = new TestChannel(CHANNEL);
    ScheduledExecutorService retryExecutor = mock(ScheduledExecutorService.class);
    MutableInstantSource instantSource =
        new MutableInstantSource(Instant.now().truncatedTo(ChronoUnit.MILLIS));
    cut =
        (InboxController)
            Inbox.builder(dataSource)
                .withInternalExecutorService(retryExecutor)
                .withInstantSource(instantSource)
                .addChannel(channel)
                .buildAndStart();

    InboxMessage message = cut.submit(inboxMessageData("recipient1"));

    Duration retryDelay = Duration.ofMillis(100);
    var context = channel.awaitProcessing("recipient1", m -> new RetryResult(m, retryDelay));
    assertStatusReached(context.message.getId(), Status.RETRY, cut, Duration.ofSeconds(10));
    var retryMessage = cut.load(message.getId());
    assertThat(retryMessage.getRetryAt()).isEqualTo(instantSource.instant().plus(retryDelay));

    var runnableCaptor = ArgumentCaptor.forClass(Runnable.class);
    verify(retryExecutor)
        .schedule(runnableCaptor.capture(), eq(retryDelay.toMillis()), eq(TimeUnit.MILLISECONDS));
    var retryAction = runnableCaptor.getValue();
    retryAction.run();
    context = channel.awaitProcessing("recipient1", ProcessingSucceededResult::new);
    assertStatusReached(context.message.getId(), Status.COMPLETED, cut, Duration.ofSeconds(10));
  }

  @Test
  void inboxDispatchingRetriesFoundInDatabase() {
    MutableInstantSource instantSource =
        new MutableInstantSource(Instant.now().truncatedTo(ChronoUnit.MILLIS));

    InboxMessageRepository repo =
        new InboxMessageRepository(new Configuration(dataSource).withInstantSource(instantSource));
    var recipientName = "recipient-to-retry";
    var message = repo.insert(new SubmitInboxMessageRequest(CHANNEL, new byte[0], recipientName));
    var retryAt = instantSource.instant().minus(100, ChronoUnit.MILLIS);
    repo.update(message, Status.RETRY, null, retryAt);

    TestChannel channel = new TestChannel(CHANNEL);
    cut =
        (InboxController)
            Inbox.builder(dataSource)
                .withInstantSource(instantSource)
                .addChannel(channel)
                .buildAndStart();

    channel.awaitProcessing(recipientName);
    assertStatusReached(message.getId(), Status.COMPLETED, cut, Duration.ofSeconds(10));
  }

  @Test
  void inboxDispatchingNewFoundInDatabase() {
    InboxMessageRepository repo = new InboxMessageRepository(new Configuration(dataSource));
    var recipientName = "recipient1";
    var message = repo.insert(new SubmitInboxMessageRequest(CHANNEL, new byte[0], recipientName));

    TestChannel channel = new TestChannel(CHANNEL);
    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();

    channel.awaitProcessing(recipientName);
    assertStatusReached(message.getId(), Status.COMPLETED, cut, Duration.ofSeconds(10));
  }

  @Test
  void inboxDispatchingWithOpenTelemetry() {
    TestChannel channel = new TestChannel(CHANNEL);

    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("inbox")
            .setParent(Context.root())
            .startSpan();

    String expectedTraceId;
    try (Scope ignore = span.makeCurrent()) {
      cut.submit(inboxMessageData("recipient1"));
      expectedTraceId = span.getSpanContext().getTraceId();
    } finally {
      span.end();
    }

    var context = channel.awaitProcessing("recipient1");
    assertThat(context.traceId).isEqualTo(expectedTraceId);
  }

  @Test
  void failedProcessingPutsMessageToErrorState() {
    FailingChannel channel = new FailingChannel(CHANNEL);
    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();

    InboxMessage message = cut.submit(inboxMessageData("recipient1"));

    assertStatusReached(message.getId(), Status.ERROR, cut, Duration.ofSeconds(10));
  }

  @Test
  void channelThrowsExceptionAndMessageGoesToErrorState() {
    ThrowingChannel channel = new ThrowingChannel(CHANNEL);
    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();

    InboxMessage message = cut.submit(inboxMessageData("recipient1"));

    assertStatusReached(message.getId(), Status.ERROR, cut, Duration.ofSeconds(10));
  }

  @Test
  void continuationProcessing() {
    ContinuationChannel channel = new ContinuationChannel(CHANNEL);
    cut = (InboxController) Inbox.builder(dataSource).addChannel(channel).buildAndStart();
    channel.setInbox(cut);

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("inbox")
            .setParent(Context.root())
            .startSpan();

    String expectedTraceId;
    InboxMessage message;
    try (Scope ignore = span.makeCurrent()) {
      expectedTraceId = span.getSpanContext().getTraceId();
      message = cut.submit(inboxMessageData("recipient1"));
    } finally {
      span.end();
    }

    assertStatusReached(
        message.getId(), Status.WAITING_FOR_CONTINUATION, cut, Duration.ofSeconds(10));
    channel.resumeProcessing();
    assertStatusReached(message.getId(), Status.COMPLETED, cut, Duration.ofSeconds(10));

    assertThat(channel.getRecordedTraceId()).isEqualTo(expectedTraceId);
  }

  @Test
  void processLargerAmountOfMessagesForDifferentRecipients() {
    FlagDrivenChannel channel = new FlagDrivenChannel(CHANNEL);

    cut =
        (InboxController)
            Inbox.builder(dataSource).addChannel(channel).withMaxConcurrency(4).buildAndStart();
    channel.setInbox(cut);

    List<String> recipients = IntStream.rangeClosed(1, 10).mapToObj(v -> "recipient" + v).toList();

    Map<String, List<Long>> messageIdsByRecipient = new HashMap<>();
    for (int i = 1; i <= 50; i++) {
      byte flag = 0;
      if (i % 10 == 0) {
        flag = 1;
      } else if (i % 25 == 0) {
        flag = 2;
      }

      for (String recipient : recipients) {
        var request =
            new SubmitInboxMessageRequest(CHANNEL, new byte[0], recipient, new byte[] {flag});

        messageIdsByRecipient
            .computeIfAbsent(recipient, k -> new ArrayList<>())
            .add(cut.submit(request).getId());
      }
    }

    messageIdsByRecipient.values().stream()
        .map(list -> list.get(list.size() - 1))
        .forEach(
            messageId ->
                assertStatusReached(messageId, Status.COMPLETED, cut, Duration.ofSeconds(60)));

    for (String recipient : recipients) {
      assertThat(channel.getRecordedIds().get(recipient))
          .containsExactlyElementsOf(messageIdsByRecipient.get(recipient));
    }
  }

  private static void assertStatusReached(
      long id, Status expectedStatus, InboxController inbox, Duration timeout) {
    long tryUntil = Instant.now().toEpochMilli() + timeout.toMillis();

    while (tryUntil > Instant.now().toEpochMilli()) {
      var message = inbox.load(id);
      if (expectedStatus.equals(message.getStatus())) {
        return;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    fail("InboxMessage{id=" + id + "} hasn't reached the expected status " + expectedStatus);
  }

  private static SubmitInboxMessageRequest inboxMessageData(String recipientName) {
    return new SubmitInboxMessageRequest(CHANNEL, new byte[0], recipientName);
  }

  private static class TestChannel implements InboxMessageChannel {

    private final String name;
    private final Map<String, CountDownLatch> firstRendezvousLatches = new ConcurrentHashMap<>();
    private final Map<String, CountDownLatch> secondRendezvousLatches = new ConcurrentHashMap<>();
    private final Map<String, InboxMessageContext> recordedContexts = new ConcurrentHashMap<>();
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
    public ProcessingResult processMessage(InboxMessage message) {
      String recipientName = message.getRecipientNames().stream().findFirst().orElseThrow();

      InboxMessageContext inboxMessageContext =
          new InboxMessageContext(message, Span.current().getSpanContext().getTraceId());
      recordedContexts.put(recipientName, inboxMessageContext);

      firstRendezvous(recipientName, false);
      secondRendezvous(recipientName, false);

      var resultProvider =
          resultProviders.computeIfAbsent(recipientName, key -> ProcessingSucceededResult::new);
      return resultProvider.apply(message);
    }

    InboxMessageContext awaitProcessing(String recipientName) {
      return awaitProcessing(recipientName, ProcessingSucceededResult::new);
    }

    InboxMessageContext awaitProcessing(
        String recipientName, Function<InboxMessage, ProcessingResult> resultProvider) {

      firstRendezvous(recipientName, true);
      InboxMessageContext recordedContext = recordedContexts.remove(recipientName);
      resultProviders.put(recipientName, resultProvider);
      secondRendezvous(recipientName, true);

      return recordedContext;
    }

    private void firstRendezvous(String recipientName, boolean reset) {
      rendezvous(firstRendezvousLatches, recipientName, reset);
    }

    private void secondRendezvous(String recipientName, boolean reset) {
      rendezvous(secondRendezvousLatches, recipientName, reset);
    }

    private void rendezvous(
        Map<String, CountDownLatch> latches, String recipientName, boolean reset) {
      var latch = latches.computeIfAbsent(recipientName, k -> new CountDownLatch(2));
      latch.countDown();
      try {
        if (!latch.await(20, TimeUnit.SECONDS)) {
          throw new IllegalStateException("No rendezvous for recipient " + recipientName);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      if (reset) {
        latches.put(recipientName, new CountDownLatch(2));
      }
    }
  }

  private record InboxMessageContext(InboxMessage message, String traceId) {}

  private record FailingChannel(String name) implements InboxMessageChannel {

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ProcessingFailedResult processMessage(InboxMessage message) {
      return new ProcessingFailedResult(message, new RuntimeException("Processing failed"));
    }
  }

  private record ThrowingChannel(String name) implements InboxMessageChannel {

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ProcessingResult processMessage(InboxMessage message) {
      throw new UnsupportedOperationException();
    }
  }

  private static class ContinuationChannel implements InboxMessageChannel {
    private final String name;
    private Inbox inbox;
    private String recordedTraceId;
    private final CountDownLatch latch = new CountDownLatch(1);

    public ContinuationChannel(String name) {
      this.name = name;
    }

    public void setInbox(Inbox inbox) {
      this.inbox = inbox;
    }

    @Override
    public String getName() {
      return name;
    }

    public String getRecordedTraceId() {
      return recordedTraceId;
    }

    @Override
    public ProcessingResult processMessage(InboxMessage message) {
      return new ContinuationResult(
          message,
          (m, reference) -> {
            try {
              if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("No rendezvous for recipient " + message);
              }
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new RuntimeException(e);
            }
            recordedTraceId = Span.current().getSpanContext().getTraceId();
            inbox.complete(reference, true);
          });
    }

    public void resumeProcessing() {
      latch.countDown();
    }
  }

  static class FlagDrivenChannel implements InboxMessageChannel {
    private final String name;
    private Inbox inbox;
    private final Map<String, List<Long>> recordedIds = new ConcurrentHashMap<>();

    public FlagDrivenChannel(String name) {
      this.name = name;
    }

    public void setInbox(Inbox inbox) {
      this.inbox = inbox;
    }

    @Override
    public String getName() {
      return name;
    }

    public Map<String, List<Long>> getRecordedIds() {
      return recordedIds;
    }

    @Override
    public ProcessingResult processMessage(InboxMessage message) {
      byte[] metadata = message.getMetadata();
      String recipient = message.getRecipientNames().stream().findFirst().orElse(null);
      List<Long> ids = recordedIds.computeIfAbsent(recipient, k -> new ArrayList<>());

      if (metadata == null) {
        throw new IllegalStateException("No metadata in message " + message);
      }

      return switch (metadata[0]) {
        case 0x01 -> new RetryResult(message, new byte[] {0}, Duration.ofMillis(100));
        case 0x02 ->
            new ContinuationResult(
                message,
                (m, reference) -> {
                  ids.add(message.getId());
                  inbox.complete(reference, true);
                });
        default -> {
          ids.add(message.getId());
          yield new ProcessingSucceededResult(message);
        }
      };
    }
  }
}
