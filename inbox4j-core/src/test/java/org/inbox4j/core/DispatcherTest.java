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

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactory;
import org.assertj.core.api.ObjectAssert;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageChannel.ProcessingFailedResult;
import org.inbox4j.core.InboxMessageChannel.ProcessingSucceededResult;
import org.inbox4j.core.InboxMessageEntity.Builder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class DispatcherTest {

  private static final String CHANNEL_NAME = "channel-name";

  Dispatcher cut;

  @AfterEach
  void tearDown() {
    cut.shutdown();
  }

  @Test
  void dispatchAndVerifySucceeded() {
    cut = dispatcher(TestChannel.succeeding());
    var message = inboxMessage();

    var actual = cut.dispatch(message);

    assertThat(actual)
        .succeedsWithin(Duration.ofSeconds(60))
        .asInstanceOf(classAndCast(ProcessingSucceededResult.class))
        .extracting(ProcessingSucceededResult::getInboxMessage)
        .isEqualTo(message);
  }

  @Test
  void dispatchAndVerifyFailed() {
    cut = dispatcher(TestChannel.failing());
    var message = inboxMessage();

    var actual = cut.dispatch(message);

    assertThat(actual)
        .succeedsWithin(Duration.ofSeconds(60))
        .asInstanceOf(classAndCast(ProcessingFailedResult.class))
        .extracting(ProcessingFailedResult::getInboxMessage)
        .isEqualTo(message);
  }

  @Test
  void unknownChannelResultsInCompletableFutureCompletedExceptionally() {
    var unknownChannel = "unknown-channel";
    cut = dispatcher(TestChannel.succeeding());
    var message = inboxMessage(unknownChannel);

    var actual = cut.dispatch(message);

    assertThat(actual)
        .failsWithin(Duration.ofSeconds(60))
        .withThrowableThat()
        .havingRootCause()
        .isExactlyInstanceOf(IllegalStateException.class)
        .withMessage(
            "InboxMessage{id=%d} refers to non existing channel: %s",
            message.getId(), unknownChannel);
  }

  @Test
  void isSupported() {
    var channel = TestChannel.succeeding();
    cut = dispatcher(channel);

    assertThat(cut.isSupported(channel.getName())).isTrue();
    assertThat(cut.isSupported(channel.getName() + "suffix")).isFalse();
  }

  private static Dispatcher dispatcher(InboxMessageChannel channel) {
    return new Dispatcher(List.of(channel), Executors.newSingleThreadExecutor(), new OtelPlugin());
  }

  private static <T> InstanceOfAssertFactory<T, ObjectAssert<T>> classAndCast(Class<T> clazz) {
    return new InstanceOfAssertFactory<>(clazz, Assertions::assertThat);
  }

  private static InboxMessage inboxMessage() {
    return inboxMessageBuilder().createInboxMessageEntity();
  }

  private static InboxMessage inboxMessage(String channelName) {
    return inboxMessageBuilder().setChannelName(channelName).createInboxMessageEntity();
  }

  private static Builder inboxMessageBuilder() {
    return new InboxMessageEntity.Builder()
        .setId(1L)
        .setCreatedAt(Instant.now())
        .setUpdatedAt(Instant.now())
        .setVersion(3)
        .setChannelName(CHANNEL_NAME)
        .setStatus(Status.IN_PROGRESS)
        .setPayload(new byte[0])
        .setAuditLog("audit-log");
  }

  private static final class TestChannel implements InboxMessageChannel {

    private final String name;
    private final Function<InboxMessage, ProcessingResult> delegate;

    static TestChannel succeeding() {
      return new TestChannel(CHANNEL_NAME, ProcessingSucceededResult::new);
    }

    static TestChannel failing() {
      return new TestChannel(
          CHANNEL_NAME,
          message ->
              new ProcessingFailedResult(
                  message,
                  message.getMetadata(),
                  new IllegalStateException("Unrecoverable error")));
    }

    public TestChannel(String name, Function<InboxMessage, ProcessingResult> delegate) {
      this.name = name;
      this.delegate = delegate;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public ProcessingResult processMessage(InboxMessage message) {
      return delegate.apply(message);
    }
  }
}
