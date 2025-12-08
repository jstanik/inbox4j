package org.inbox4j;

import static java.time.temporal.ChronoUnit.MILLIS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.inbox4j.InboxMessage.Status;
import org.junit.jupiter.api.Test;

class InboxMessageRepositoryTest extends AbstractDatabaseTest {

  MutableInstantSource instantSource = new MutableInstantSource(Instant.now().truncatedTo(MILLIS));

  @Test
  void insert() {
    InboxMessageRepository cut = new InboxMessageRepository(dataSource, instantSource);
    InboxMessageData data = createInboxMessageData();

    InboxMessageEntity entity = (InboxMessageEntity) cut.insert(data);

    assertThat(entity.getId()).isPositive();
    assertThat(entity.getVersion()).isEqualTo(1);
    assertThat(entity.getChannelName()).isEqualTo(data.getChannelName());
    assertThat(entity.getCreatedAt()).isEqualTo(instantSource.instant());
    assertThat(entity.getUpdatedAt()).isEqualTo(instantSource.instant());
    assertThat(entity.getMetadata()).isEqualTo(data.getMetadata());
    assertThat(entity.getPayload()).isEqualTo(data.getPayload());
    assertThat(entity.getStatus()).isEqualTo(Status.NEW);
    assertThat(entity.getTargetNames()).isEqualTo(data.getTargetNames());
    assertThat(entity.getAuditLog()).isEqualTo(instantSource.instant() + " - message inserted");
  }

  @Test
  void insertFailsWhenNoTargetNameProvided() {
    InboxMessageRepository cut = new InboxMessageRepository(dataSource, instantSource);
    InboxMessageData data = new InboxMessageData("channel", new byte[0], List.of(), new byte[0]);

    assertThatThrownBy(() -> cut.insert(data))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("No target name provided for new inbox message");
  }

  @Test
  void update() {
    InboxMessageRepository cut = new InboxMessageRepository(dataSource, instantSource);
    InboxMessage message = cut.insert(createInboxMessageData());

    Instant updateInstant = instantSource.instant().plusSeconds(10);
    instantSource.setInstant(updateInstant);

    byte[] updatedMetadata = {(byte) 0xca, (byte) 0xfe, (byte) 0xba, (byte) 0xbe};
    InboxMessageEntity updatedMessage = (InboxMessageEntity) cut.update(message,
        Status.IN_PROGRESS, updatedMetadata);

    assertThat(updatedMessage.getId()).isEqualTo(message.getId());
    assertThat(updatedMessage.getVersion()).isEqualTo(message.getVersion() + 1);
    assertThat(updatedMessage.getStatus()).isEqualTo(Status.IN_PROGRESS);
    assertThat(updatedMessage.getMetadata()).isEqualTo(updatedMetadata);
    assertThat(updatedMessage.getUpdatedAt()).isEqualTo(updateInstant);
  }

  @Test
  void updateWithNullMetadataClearsMetadata() {
    InboxMessageRepository cut = new InboxMessageRepository(dataSource, instantSource);
    InboxMessage message = cut.insert(
        new InboxMessageData("channel", new byte[0], "targetName", new byte[]{65}));

    InboxMessageEntity updatedMessage = (InboxMessageEntity) cut.update(message,
        Status.IN_PROGRESS, null);

    assertThat(updatedMessage.getMetadata()).isNull();
  }

  @Test
  void updateDetectsStaleDataUpdate() {
    InboxMessageRepository cut = new InboxMessageRepository(dataSource, instantSource);
    InboxMessage message = cut.insert(createInboxMessageData());
    cut.update(message, Status.IN_PROGRESS, new byte[0]);

    assertThatThrownBy(() -> cut.update(message, Status.COMPLETED, new byte[0]))
        .isInstanceOf(StaleDataUpdateException.class);
  }

  @Test
  void findAllProcessingRelevant() {
    var cut = new InboxMessageRepository(dataSource, instantSource);
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

    assertThat(actual).extracting(InboxMessageReferences::id)
        .containsExactly(messageA.getId(), messageC.getId());
  }

  @Test
  void findNextProcessingRelevantReturnEmptyWhenInCompletedState() {
    var cut = new InboxMessageRepository(dataSource, instantSource);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnEmptyWhenInInProgressState() {
    var cut = new InboxMessageRepository(dataSource, instantSource);
    setUpInboxMessage(cut, Status.IN_PROGRESS, "A-1", "A-2");
    setUpInboxMessage(cut, Status.NEW, "A-1",
        "A-2"); // this should not be returned as it entered inbox after the previous message

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).isEmpty();
  }

  @Test
  void findNextProcessingRelevantReturnFirstInNewState() {
    var cut = new InboxMessageRepository(dataSource, instantSource);
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    setUpInboxMessage(cut, Status.ERROR, "A-1", "A-2");
    setUpInboxMessage(cut, Status.COMPLETED, "A-1", "A-2");
    var expectedMessage = setUpInboxMessage(cut, Status.NEW, "A-1", "A-2", "A-3");
    setUpInboxMessage(cut, Status.NEW, "A-1",
        "A-2"); // this entered inbox after the previous it means it should not be returned

    var actual = cut.findNextProcessingRelevant(List.of("A-1", "A-2"));

    assertThat(actual).hasValueSatisfying(
        actualMessage -> assertThat(actualMessage.getId()).isEqualTo(expectedMessage.getId()));

  }

  private static InboxMessage setUpInboxMessage(InboxMessageRepository repository, Status status,
      String... targetNames) {
    var data = new InboxMessageData("channel-1", new byte[]{65, 66, 67, 68},
        Arrays.asList(targetNames), new byte[0]);
    var message = repository.insert(data);
    return repository.update(message, status, message.getMetadata());
  }

  private static InboxMessageData createInboxMessageData() {
    return new InboxMessageData("channel-1", new byte[]{65, 66},
        List.of("name-1", "name-2"), new byte[]{67, 68});
  }
}