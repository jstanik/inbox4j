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

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageRepository.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;

class InboxMessageRepositoryTest extends AbstractDatabaseTest {

  MutableInstantSource instantSource = new MutableInstantSource(Instant.now().truncatedTo(MILLIS));
  Configuration configuration =
      new Configuration(dataSource).withInstantSource(instantSource).withOtelPlugin(otelPlugin);

  @Test
  void insert() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    MessageInsertionRequest request = messageInsertionRequest();

    InboxMessageEntity entity = (InboxMessageEntity) cut.insert(request);

    assertThat(entity.getId()).isPositive();
    assertThat(entity.getVersion()).isEqualTo(1);
    assertThat(entity.getChannelName()).isEqualTo(request.getChannelName());
    assertThat(entity.getCreatedAt()).isEqualTo(instantSource.instant());
    assertThat(entity.getUpdatedAt()).isEqualTo(instantSource.instant());
    assertThat(entity.getMetadata()).isEqualTo(request.getMetadata());
    assertThat(entity.getPayload()).isEqualTo(request.getPayload());
    assertThat(entity.getStatus()).isEqualTo(Status.NEW);
    assertThat(entity.getRecipientNames()).isEqualTo(request.getRecipientNames());
    assertThat(entity.getAuditLog()).isEqualTo(instantSource.instant() + " - message inserted");
  }

  @Test
  void insertWithOtelEnabled() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    MessageInsertionRequest request = messageInsertionRequest();

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("inbox")
            .setParent(Context.root())
            .startSpan();

    InboxMessageEntity entity;
    try (Scope ignore = span.makeCurrent()) {
      entity = (InboxMessageEntity) cut.insert(request);
    } finally {
      span.end();
    }
    assertThat(entity.getTraceContext()).startsWith("traceparent:");
  }

  @Test
  void insertWithOtelDisabled() {
    InboxMessageRepository cut =
        new InboxMessageRepository(configuration.withOtelPlugin(new OtelPlugin(false)));
    MessageInsertionRequest request = messageInsertionRequest();
    var entity = (InboxMessageEntity) cut.insert(request);

    assertThat(entity.getTraceContext()).isNull();
  }

  @Test
  void insertFailsWhenNoRecipientNameProvided() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    MessageInsertionRequest data =
        new MessageInsertionRequest("channel", new byte[0], List.of(), new byte[0]);

    assertThatThrownBy(() -> cut.insert(data))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No recipient name provided for new inbox message");
  }

  @Test
  void update() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());

    Instant updateInstant = instantSource.instant().plusSeconds(10);
    instantSource.setInstant(updateInstant);

    byte[] updatedMetadata = {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe};
    InboxMessageEntity updatedMessage =
        (InboxMessageEntity) cut.update(message, Status.IN_PROGRESS, updatedMetadata, null);

    assertThat(updatedMessage.getId()).isEqualTo(message.getId());
    assertThat(updatedMessage.getVersion()).isEqualTo(message.getVersion() + 1);
    assertThat(updatedMessage.getStatus()).isEqualTo(Status.IN_PROGRESS);
    assertThat(updatedMessage.getMetadata()).isEqualTo(updatedMetadata);
    assertThat(updatedMessage.getUpdatedAt()).isEqualTo(updateInstant);
  }

  @Test
  void updateWithNullMetadataClearsMetadata() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message =
        cut.insert(
            new MessageInsertionRequest("channel", new byte[0], "recipientName", new byte[] {65}));

    InboxMessageEntity updatedMessage =
        (InboxMessageEntity) cut.update(message, Status.IN_PROGRESS, null, null);

    assertThat(updatedMessage.getMetadata()).isNull();
  }

  @Test
  void updateDetectsStaleDataUpdate() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    cut.update(message, Status.IN_PROGRESS, new byte[0], null);

    assertThatThrownBy(() -> cut.update(message, Status.COMPLETED, new byte[0], null))
        .isInstanceOf(StaleDataUpdateException.class);
  }

  @Test
  void updateToRetryStateRequiresRetryAtInstant() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());

    assertThatThrownBy(() -> cut.update(message, Status.RETRY, new byte[0], null))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void updateToRetryState() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);

    InboxMessage actual = cut.update(message, Status.RETRY, new byte[0], retryAt);

    assertThat(actual.getStatus()).isEqualTo(Status.RETRY);
    assertThat(actual.getRetryAt()).isEqualTo(retryAt);
  }

  @Test
  void updateFromRetryStateRequiresClearRetryAtInstant() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);
    InboxMessage retryMessage = cut.update(message, Status.RETRY, new byte[0], retryAt);

    Instant someInstant = Instant.now();
    assertThatThrownBy(() -> cut.update(retryMessage, Status.COMPLETED, new byte[0], someInstant))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void updateFromRetryState() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);
    InboxMessage retryMessage = cut.update(message, Status.RETRY, new byte[0], retryAt);

    InboxMessage actual = cut.update(retryMessage, Status.COMPLETED, new byte[0], null);

    assertThat(actual.getStatus()).isEqualTo(Status.COMPLETED);
    assertThat(actual.getRetryAt()).isNull();
  }

  @Test
  void findAllProcessingRelevant_returnsOnlyFirstAfterCompletedIfItsInNewStatus() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    var messageA = setUpInboxMessage(cut, Status.NEW, "A-1", "A-2");
    setUpInboxMessage(cut, Status.NEW, "A-1");
    setUpInboxMessage(cut, Status.NEW, "A-2");
    setUpInboxMessage(cut, Status.COMPLETED, "B-1", "B-2");
    setUpInboxMessage(cut, Status.IN_PROGRESS, "B-2");
    setUpInboxMessage(cut, Status.NEW, "B-1", "B-2");
    setUpInboxMessage(cut, Status.ERROR, "C-1", "C-2");
    setUpInboxMessage(cut, Status.NEW, "C-1", "C-3");
    setUpInboxMessage(cut, Status.COMPLETED, "D-1", "D-2");
    setUpInboxMessage(cut, Status.RETRY, "D-2");
    setUpInboxMessage(cut, Status.COMPLETED, "E-1", "E-2");
    setUpInboxMessage(cut, Status.WAITING_FOR_CONTINUATION, "E-2");

    var actual = cut.findAllProcessingRelevant();

    assertThat(actual).extracting(InboxMessageView::id).containsExactly(messageA.getId());
  }

  @ParameterizedTest
  @EnumSource(
      value = Status.class,
      names = {"COMPLETED", "NEW"},
      mode = Mode.EXCLUDE)
  void findAllProcessingRelevantFindsNothingIfNonCompleteStatusAfterTerminalStatus(
      Status intermediateStatus) {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "R1");
    setUpInboxMessage(cut, intermediateStatus, "R1");

    var actual = cut.findAllProcessingRelevant();

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnEmptyWhenInCompletedState() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(
      value = Status.class,
      names = {"COMPLETED", "NEW"},
      mode = Mode.EXCLUDE)
  void findNextProcessingRelevantReturnEmptyWhenInNonCompleteStatusAfterCompleteStatus(
      Status intermediateStatus) {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    setUpInboxMessage(cut, intermediateStatus, "A-1", "A-2");
    setUpInboxMessage(
        cut,
        Status.NEW,
        "A-1",
        "A-2"); // this should not be returned as it entered inbox after the previous message in
    // intermediate status

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnFirstInNewState() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    var expectedMessage = setUpInboxMessage(cut, Status.NEW, "A-1", "A-2", "A-3");
    setUpInboxMessage(
        cut,
        Status.NEW,
        "A-1",
        "A-2"); // this entered inbox after the previous it means it should not be returned

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual)
        .hasValueSatisfying(
            actualMessage -> assertThat(actualMessage.getId()).isEqualTo(expectedMessage.getId()));
  }

  @Test
  void reset() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    cut.update(message, Status.ERROR, new byte[0], null);

    cut.reset(message.getId());

    var actual = cut.load(message.getId());
    assertThat(actual.getStatus()).isEqualTo(Status.NEW);
  }

  @ParameterizedTest
  @EnumSource(
      names = {"ERROR"},
      mode = Mode.EXCLUDE)
  void resetFailsWhenOtherThanErrorState(Status initialStatus) {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(messageInsertionRequest());
    if (initialStatus == Status.RETRY) {
      cut.update(message, initialStatus, new byte[0], Instant.now());
    } else {
      cut.update(message, initialStatus, new byte[0], null);
    }
    long messageId = message.getId();

    assertThatThrownBy(() -> cut.reset(messageId))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Reset failed because InboxMessage{id=%d} didn't have ERROR status", message.getId());
  }

  @Test
  void delete() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    long messageId = cut.insert(messageInsertionRequest()).getId();

    cut.delete(messageId);

    assertThatThrownBy(() -> cut.load(messageId))
        .isInstanceOf(DatabaseAccessException.class)
        .hasMessage("Inbox message id=%s not found", messageId);
  }

  @Test
  void deleteInboxMessagesCompletedBefore() {
    Instant instant1 = instantSource.instant();
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    var message1IdCompleted = setUpInboxMessage(cut, Status.COMPLETED, "name").getId();
    var message2IdInProgress = setUpInboxMessage(cut, Status.IN_PROGRESS, "name").getId();
    var instant2 = instant1.plusMillis(1);
    instantSource.setInstant(instant2);
    var message3IdCompleted = setUpInboxMessage(cut, Status.COMPLETED, "name").getId();

    cut.deleteInboxMessagesCompletedBefore(instant2);

    assertDoesNotThrow(
        () -> {
          cut.load(message2IdInProgress);
          cut.load(message3IdCompleted);
        });
    assertThatThrownBy(() -> cut.load(message1IdCompleted))
        .isInstanceOf(DatabaseAccessException.class)
        .hasMessage("Inbox message id=%s not found", message1IdCompleted);
  }

  @Test
  void customTableNames() {
    var cut =
        new InboxMessageRepository(
            new Configuration(
                    postgresDataSource(
                        "jdbc:tc:postgresql:18.1:///postgres?TC_INITSCRIPT=sql/postgresql-custom-table-names-ddl.sql"))
                .withTableInboxMessage("inbox_message_custom")
                .withTableInboxMessageRecipient("inbox_message_recipient_custom"));

    MessageInsertionRequest request = messageInsertionRequest();

    assertThatCode(() -> cut.insert(request)).doesNotThrowAnyException();
  }

  private static InboxMessage setUpInboxMessage(
      InboxMessageRepository repository, Status status, String... recipientNames) {
    var data =
        new MessageInsertionRequest(
            "channel-1", new byte[] {65, 66, 67, 68}, Arrays.asList(recipientNames), new byte[0]);
    var message = repository.insert(data);
    Instant retryAt = null;
    if (status == Status.RETRY) {
      retryAt = Instant.now();
    }
    return repository.update(message, status, message.getMetadata(), retryAt);
  }

  private static MessageInsertionRequest messageInsertionRequest() {
    return new MessageInsertionRequest(
        "channel-1", new byte[] {65, 66}, List.of("name-1", "name-2"), new byte[] {67, 68});
  }
}
