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

class InboxMessageRepositoryTest extends AbstractDatabaseTest {

  MutableInstantSource instantSource = new MutableInstantSource(Instant.now().truncatedTo(MILLIS));
  Configuration configuration =
      new Configuration(dataSource).withInstantSource(instantSource).withOtelPlugin(otelPlugin);

  @Test
  void insert() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    MessageInsertionRequest request = builderMessageInsertionRequest();

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
    MessageInsertionRequest request = builderMessageInsertionRequest();

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
    MessageInsertionRequest request = builderMessageInsertionRequest();
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
    InboxMessage message = cut.insert(builderMessageInsertionRequest());

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
    InboxMessage message = cut.insert(builderMessageInsertionRequest());
    cut.update(message, Status.IN_PROGRESS, new byte[0], null);

    assertThatThrownBy(() -> cut.update(message, Status.COMPLETED, new byte[0], null))
        .isInstanceOf(StaleDataUpdateException.class);
  }

  @Test
  void updateToRetryStateRequiresRetryAtInstant() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(builderMessageInsertionRequest());

    assertThatThrownBy(() -> cut.update(message, Status.RETRY, new byte[0], null))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void updateToRetryState() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(builderMessageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);

    InboxMessage actual = cut.update(message, Status.RETRY, new byte[0], retryAt);

    assertThat(actual.getStatus()).isEqualTo(Status.RETRY);
    assertThat(actual.getRetryAt()).isEqualTo(retryAt);
  }

  @Test
  void updateFromRetryStateRequiresClearRetryAtInstant() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(builderMessageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);
    InboxMessage retryMessage = cut.update(message, Status.RETRY, new byte[0], retryAt);

    Instant someInstant = Instant.now();
    assertThatThrownBy(() -> cut.update(retryMessage, Status.COMPLETED, new byte[0], someInstant))
        .isInstanceOf(DataIntegrityViolationException.class);
  }

  @Test
  void updateFromRetryState() {
    InboxMessageRepository cut = new InboxMessageRepository(configuration);
    InboxMessage message = cut.insert(builderMessageInsertionRequest());
    Instant retryAt = instantSource.instant().plusSeconds(10);
    InboxMessage retryMessage = cut.update(message, Status.RETRY, new byte[0], retryAt);

    InboxMessage actual = cut.update(retryMessage, Status.COMPLETED, new byte[0], null);

    assertThat(actual.getStatus()).isEqualTo(Status.COMPLETED);
    assertThat(actual.getRetryAt()).isNull();
  }

  @Test
  void findAllProcessingRelevant() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    var messageA = setUpInboxMessage(cut, Status.NEW, "A-1", "A-2");
    setUpInboxMessage(cut, Status.NEW, "A-1");
    setUpInboxMessage(cut, Status.NEW, "A-2");
    setUpInboxMessage(cut, Status.COMPLETED, "B-1", "B-2");
    setUpInboxMessage(cut, Status.IN_PROGRESS, "B-2");
    setUpInboxMessage(cut, Status.NEW, "B-1", "B-2");
    setUpInboxMessage(cut, Status.ERROR, "C-1", "C-2");
    var messageC = setUpInboxMessage(cut, Status.NEW, "C-1", "C-3");

    var actual = cut.findAllProcessingRelevant();

    assertThat(actual)
        .extracting(InboxMessageView::id)
        .containsExactly(messageA.getId(), messageC.getId());
  }

  @Test
  void findNextProcessingRelevantReturnEmptyWhenInCompletedState() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnEmptyWhenInInProgressState() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.IN_PROGRESS, "A-1", "A-2");
    setUpInboxMessage(
        cut,
        Status.NEW,
        "A-1",
        "A-2"); // this should not be returned as it entered inbox after the previous message

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnFirstInNewState() {
    var cut = new InboxMessageRepository(configuration);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    setUpInboxMessage(cut, Status.ERROR, "A-1", "A-2");
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
  void customTableNames() {
    var cut =
        new InboxMessageRepository(
            new Configuration(
                    postgresDataSource(
                        "jdbc:tc:postgresql:18.1:///postgres?TC_INITSCRIPT=sql/postgresql-custom-table-names-ddl.sql"))
                .withTableInboxMessage("inbox_message_custom")
                .withTableInboxMessageRecipient("inbox_message_recipient_custom"));

    MessageInsertionRequest request = builderMessageInsertionRequest();

    assertThatCode(() -> cut.insert(request)).doesNotThrowAnyException();
  }

  private static InboxMessage setUpInboxMessage(
      InboxMessageRepository repository, Status status, String... recipientNames) {
    var data =
        new MessageInsertionRequest(
            "channel-1", new byte[] {65, 66, 67, 68}, Arrays.asList(recipientNames), new byte[0]);
    var message = repository.insert(data);
    return repository.update(message, status, message.getMetadata(), null);
  }

  private static MessageInsertionRequest builderMessageInsertionRequest() {
    return new MessageInsertionRequest(
        "channel-1", new byte[] {65, 66}, List.of("name-1", "name-2"), new byte[] {67, 68});
  }
}
