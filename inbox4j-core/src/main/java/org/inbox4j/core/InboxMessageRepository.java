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

import static org.inbox4j.core.TransactionTemplate.transaction;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.sql.DataSource;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageEntity.Builder;

class InboxMessageRepository {

  private static final String SQL_SELECT_INBOX_MESSAGE =
      """
      SELECT message.id,
             message.version,
             message.created_at,
             message.updated_at,
             message.channel,
             message.status,
             message.payload,
             message.metadata,
             message.retry_at,
             message.trace_context,
             message.audit_log,
             recipient.name
        FROM inbox_message message
        LEFT JOIN inbox_message_recipient recipient ON recipient.inbox_message_fk = message.id
       WHERE message.id = ?
      """;

  @SuppressWarnings("java:S5665")
  private static final String SQL_UPDATE_INBOX_MESSAGE =
      """
      UPDATE inbox_message
         SET status = ?,
             metadata = ?,
             retry_at = ?,
             version = version + 1,
             updated_at = ?,
             audit_log = audit_log || '\n' || ?
       WHERE id = ?
         AND version = ?
      """;

  private static final String SQL_FIND_ALL_PROCESSING_RELEVANT =
      """
      SELECT message.id,
             message.status,
             recipient.name
        FROM inbox_message message
        JOIN inbox_message_recipient recipient ON recipient.inbox_message_fk = message.id
       WHERE message.status NOT IN ('COMPLETED', 'ERROR', 'RETRY')
       ORDER BY message.id ASC
      """;

  private static final String SQL_FIND_NEXT_PROCESSING_RELEVANT_BY_RECIPIENT_NAME =
      """
      SELECT message.id,
             message.status,
             recipient.name
        FROM inbox_message message
        JOIN inbox_message_recipient recipient ON recipient.inbox_message_fk = message.id
       WHERE message.status NOT IN ('COMPLETED', 'ERROR', 'RETRY')
         AND recipient.name IN ($$name_placeholders$$)
       ORDER BY message.id ASC
      """;

  private static final String SQL_INSERT_INBOX_MESSAGE =
      """
      INSERT INTO inbox_message(
               version,
               created_at,
               updated_at,
               channel,
               status,
               payload,
               metadata,
               trace_context,
               audit_log
             )
             VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
      """;
  private static final String SQL_INSERT_INBOX_MESSAGE_RECIPIENT =
      "INSERT INTO inbox_message_recipient (name, inbox_message_fk) VALUES (?, ?)";

  private static final String COLUMN_ID = "id";
  private static final String COLUMN_STATUS = "status";
  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_VERSION = "version";
  private static final String COLUMN_CREATED_AT = "created_at";
  private static final String COLUMN_UPDATED_AT = "updated_at";
  private static final String COLUMN_CHANNEL = "channel";
  private static final String COLUMN_PAYLOAD = "payload";
  private static final String COLUMN_METADATA = "metadata";
  private static final String COLUMN_RETRY_AT = "retry_at";
  private static final String COLUMN_TRACE_CONTEXT = "trace_context";
  private static final String COLUMN_AUDIT_LOG = "audit_log";

  private final DataSource dataSource;
  private final InstantSource instantSource;
  private final OtelPlugin otelPlugin;

  InboxMessageRepository(
      DataSource dataSource, InstantSource instantSource, OtelPlugin otelPlugin) {
    this.dataSource = dataSource;
    this.instantSource = instantSource;
    this.otelPlugin = otelPlugin;
  }

  InboxMessage insert(MessageInsertionRequest data) {
    if (data.getRecipientNames().isEmpty()) {
      throw new IllegalArgumentException("No recipient name provided for new inbox message");
    }
    return transaction(
        dataSource,
        connection -> {
          var inboxMessageId = insertInboxMessage(connection, data);
          insertInboxMessageRecipient(connection, inboxMessageId, data.getRecipientNames());
          return loadInboxMessageEntity(connection, inboxMessageId);
        });
  }

  InboxMessage load(long id) {
    return transaction(dataSource, connection -> loadInboxMessageEntity(connection, id));
  }

  InboxMessage update(InboxMessage message, Status newStatus, byte[] metadata, Instant retryAt) {
    return transaction(
        dataSource,
        connection -> {
          int updatedRows;
          try (var statement = connection.prepareStatement(SQL_UPDATE_INBOX_MESSAGE)) {
            var currentInstant = instantSource.instant();
            int index = 1;
            statement.setString(index++, newStatus.name());
            if (metadata == null) {
              statement.setNull(index++, Types.BINARY);
            } else {
              statement.setBytes(index++, metadata);
            }
            if (retryAt == null) {
              statement.setNull(index++, Types.TIMESTAMP_WITH_TIMEZONE);
            } else {
              statement.setTimestamp(index++, new Timestamp(retryAt.toEpochMilli()));
            }
            statement.setTimestamp(index++, new Timestamp(currentInstant.toEpochMilli()));
            statement.setString(index++, auditLog(currentInstant, message, newStatus, metadata));
            statement.setLong(index++, message.getId());
            statement.setInt(index, message.getVersion());

            updatedRows = statement.executeUpdate();
          }

          var loadedEntity = loadInboxMessageEntity(connection, message.getId());
          if (updatedRows == 0) {
            throw new StaleDataUpdateException(
                "InboxMessage{id="
                    + message.getId()
                    + "}: expected version "
                    + message.getVersion()
                    + " but found version "
                    + loadedEntity.getVersion());
          }

          return loadedEntity;
        });
  }

  List<InboxMessageView> findAllProcessingRelevant() {
    return transaction(
        dataSource,
        connection -> {
          try (var statement = connection.prepareStatement(SQL_FIND_ALL_PROCESSING_RELEVANT)) {
            try (ResultSet resultSet = statement.executeQuery()) {
              if (!resultSet.next()) {
                return List.of();
              }

              Set<String> visitedNames = new HashSet<>();
              List<InboxMessageView> result = new ArrayList<>();
              InboxMessageView.Builder builder =
                  new InboxMessageView.Builder(
                      resultSet.getLong(COLUMN_ID),
                      Status.forName(resultSet.getString(COLUMN_STATUS)));
              do {
                long messageId = resultSet.getLong(COLUMN_ID);
                if (builder.id != messageId) {
                  addProcessingRelevant(visitedNames, result, builder);
                  var status = Status.forName(resultSet.getString(COLUMN_STATUS));
                  builder = new InboxMessageView.Builder(messageId, status);
                }
                String recipientName = resultSet.getString(COLUMN_NAME);
                builder.addName(recipientName);
              } while (resultSet.next());
              addProcessingRelevant(visitedNames, result, builder);
              return List.copyOf(result);
            }
          }
        });
  }

  Optional<InboxMessage> findNextProcessingRelevant(Collection<String> recipientNames) {
    if (recipientNames.isEmpty()) {
      throw new IllegalArgumentException("No recipient names provided");
    }

    return transaction(
        dataSource,
        connection -> {
          var placeholders = String.join(",", Collections.nCopies(recipientNames.size(), "?"));

          try (var statement =
              connection.prepareStatement(
                  SQL_FIND_NEXT_PROCESSING_RELEVANT_BY_RECIPIENT_NAME.replace(
                      "$$name_placeholders$$", placeholders))) {

            int index = 1;
            for (String recipientName : recipientNames) {
              statement.setString(index++, recipientName);
            }

            try (var resultSet = statement.executeQuery()) {
              if (!resultSet.next()) {
                return Optional.empty();
              }

              long id = resultSet.getLong(COLUMN_ID);
              Status status = Status.forName(resultSet.getString(COLUMN_STATUS));

              if (Status.NEW.equals(status)) {
                return Optional.of(loadInboxMessageEntity(connection, id));
              } else {
                return Optional.empty();
              }
            }
          }
        });
  }

  private void addProcessingRelevant(
      Set<String> visitedNames,
      List<InboxMessageView> references,
      InboxMessageView.Builder builder) {
    boolean shouldAdd =
        builder.status.equals(Status.NEW)
            && builder.names.stream().noneMatch(visitedNames::contains);
    visitedNames.addAll(builder.names);

    if (shouldAdd) {
      references.add(builder.build());
    }
  }

  private long insertInboxMessage(Connection connection, MessageInsertionRequest data)
      throws SQLException {

    try (var statement =
        connection.prepareStatement(SQL_INSERT_INBOX_MESSAGE, Statement.RETURN_GENERATED_KEYS)) {
      var currentInstant = instantSource.instant();
      int index = 1;
      statement.setTimestamp(index++, new Timestamp(currentInstant.toEpochMilli()));
      statement.setTimestamp(index++, new Timestamp(currentInstant.toEpochMilli()));
      statement.setString(index++, data.getChannelName());
      statement.setString(index++, Status.NEW.name());
      statement.setBytes(index++, data.getPayload());
      var metadata = data.getMetadata();
      if (metadata == null) {
        statement.setNull(index++, Types.BINARY);
      } else {
        statement.setBytes(index++, metadata);
      }
      otelPlugin.getContextInjector().inject(statement, index++);
      statement.setString(index, auditLog(currentInstant, "message inserted"));
      statement.execute();
      var resultSet = statement.getGeneratedKeys();
      resultSet.next();
      return resultSet.getLong(1);
    }
  }

  private void insertInboxMessageRecipient(
      Connection connection, long inboxMessageId, Set<String> recipientNames) throws SQLException {
    try (var statement = connection.prepareStatement(SQL_INSERT_INBOX_MESSAGE_RECIPIENT)) {
      statement.setLong(2, inboxMessageId);
      for (String name : recipientNames) {
        statement.setString(1, name);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  private InboxMessageEntity loadInboxMessageEntity(Connection connection, long id)
      throws SQLException {

    try (var statement = connection.prepareStatement(SQL_SELECT_INBOX_MESSAGE)) {
      statement.setLong(1, id);

      try (var resultSet = statement.executeQuery()) {
        if (!resultSet.next()) {
          throw new DatabaseAccessException("Inbox message id=" + id + " not found.");
        }

        InboxMessageEntity.Builder builder = null;
        do {
          if (builder == null) {
            builder =
                new Builder()
                    .setId(resultSet.getLong(COLUMN_ID))
                    .setVersion(resultSet.getInt(COLUMN_VERSION))
                    .setCreatedAt(toInstant(resultSet.getTimestamp(COLUMN_CREATED_AT)))
                    .setUpdatedAt(toInstant(resultSet.getTimestamp(COLUMN_UPDATED_AT)))
                    .setChannelName(resultSet.getString(COLUMN_CHANNEL))
                    .setStatus(toStatus(resultSet.getString(COLUMN_STATUS)))
                    .setPayload(resultSet.getBytes(COLUMN_PAYLOAD))
                    .setMetadata(resultSet.getBytes(COLUMN_METADATA))
                    .setRetryAt(toInstant(resultSet.getTimestamp(COLUMN_RETRY_AT)))
                    .setTraceContext(resultSet.getString(COLUMN_TRACE_CONTEXT))
                    .setAuditLog(resultSet.getString(COLUMN_AUDIT_LOG));
          }
          builder.addRecipientName(resultSet.getString(COLUMN_NAME));
        } while (resultSet.next());
        return builder.createInboxMessageEntity();
      }
    }
  }

  private static Instant toInstant(Timestamp timestamp) {
    return timestamp == null ? null : timestamp.toInstant();
  }

  private static Status toStatus(String statusName) {
    for (Status status : Status.values()) {
      if (status.name().equals(statusName)) {
        return status;
      }
    }

    throw new IllegalArgumentException("Unsupported status value: " + statusName);
  }

  private static String auditLog(Instant instant, String text) {
    return instant + " - " + text;
  }

  private static String auditLog(
      Instant instant, InboxMessage original, Status newStatus, byte[] newMetadata) {
    List<String> parts = new ArrayList<>(2);

    if (!original.getStatus().equals(newStatus)) {
      parts.add("status " + original.getStatus() + " -> " + newStatus);
    }

    if (!Arrays.equals(original.getMetadata(), newMetadata)) {
      if (newMetadata == null) {
        parts.add("metadata removed");
      } else if (original.getMetadata() == null) {
        parts.add("metadata set");
      } else {
        parts.add("metadata changed");
      }
    }

    return auditLog(instant, String.join("; ", parts));
  }
}
