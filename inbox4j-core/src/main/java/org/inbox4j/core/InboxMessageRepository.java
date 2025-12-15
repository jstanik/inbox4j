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

import static java.util.Objects.requireNonNull;
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
import java.util.function.Consumer;
import javax.sql.DataSource;
import org.inbox4j.core.InboxMessage.Status;
import org.inbox4j.core.InboxMessageEntity.Builder;

class InboxMessageRepository {

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
  private final Sqls sqls;

  InboxMessageRepository(Configuration configuration) {
    this.dataSource = configuration.dataSource;
    this.instantSource = configuration.instantSource;
    this.otelPlugin = configuration.otelPlugin;
    this.sqls = new Sqls(configuration.tableInboxMessage, configuration.tableInboxMessageRecipient);
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
          try (var statement = connection.prepareStatement(sqls.getUpdateInboxMessage())) {
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
          try (var statement = connection.prepareStatement(sqls.getFindAllProcessingRelevant())) {
            try (ResultSet resultSet = statement.executeQuery()) {
              Set<String> visitedNames = new HashSet<>();
              List<InboxMessageView> result = new ArrayList<>();

              mapInboxMessageViews(
                  resultSet, view -> addProcessingRelevant(visitedNames, result, view));

              return List.copyOf(result);
            }
          }
        });
  }

  private void addProcessingRelevant(
      Set<String> visitedNames, List<InboxMessageView> references, InboxMessageView view) {

    var names = view.recipient().names();
    boolean shouldAdd =
        view.status().equals(Status.NEW) && names.stream().noneMatch(visitedNames::contains);
    visitedNames.addAll(names);

    if (shouldAdd) {
      references.add(view);
    }
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
                  sqls.getFindNextProcessingRelevantByRecipientName()
                      .replace("$$name_placeholders$$", placeholders))) {

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

  private long insertInboxMessage(Connection connection, MessageInsertionRequest data)
      throws SQLException {

    try (var statement =
        connection.prepareStatement(
            sqls.getInsertInboxMessage(), Statement.RETURN_GENERATED_KEYS)) {
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
    try (var statement = connection.prepareStatement(sqls.getInsertInboxMessageRecipient())) {
      statement.setLong(2, inboxMessageId);
      for (String name : recipientNames) {
        statement.setString(1, name);
        statement.addBatch();
      }
      statement.executeBatch();
    }
  }

  List<InboxMessageView> findInboxMessagesForRetry() {
    return transaction(
        dataSource,
        connection -> {
          try (var statement = connection.prepareStatement(sqls.getFindForRetry())) {
            try (ResultSet resultSet = statement.executeQuery()) {
              List<InboxMessageView> result = new ArrayList<>();
              mapInboxMessageViews(resultSet, result::add);
              return List.copyOf(result);
            }
          }
        });
  }

  private void mapInboxMessageViews(ResultSet resultSet, Consumer<InboxMessageView> consumer)
      throws SQLException {
    if (!resultSet.next()) {
      return;
    }

    InboxMessageView.Builder builder =
        new InboxMessageView.Builder(
            resultSet.getLong(COLUMN_ID),
            Status.forName(resultSet.getString(COLUMN_STATUS)),
            toInstant(resultSet.getTimestamp(COLUMN_RETRY_AT)));
    do {
      long messageId = resultSet.getLong(COLUMN_ID);
      Instant retryAt = toInstant(resultSet.getTimestamp(COLUMN_RETRY_AT));
      if (builder.id != messageId) {
        consumer.accept(builder.build());
        var status = Status.forName(resultSet.getString(COLUMN_STATUS));
        builder = new InboxMessageView.Builder(messageId, status, retryAt);
      }
      String recipientName = resultSet.getString(COLUMN_NAME);
      builder.addName(recipientName);
    } while (resultSet.next());
    consumer.accept(builder.build());
  }

  private InboxMessageEntity loadInboxMessageEntity(Connection connection, long id)
      throws SQLException {

    try (var statement = connection.prepareStatement(sqls.getSelectInboxMessage())) {
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

  private static class Sqls {

    private static final String SELECT_ALL_FIELDS =
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
        """;

    private static final String SELECT_VIEW_FIELDS =
        """
        SELECT message.id,
               message.status,
               message.retry_at,
               recipient.name
        """;

    private static final String SQL_SELECT_INBOX_MESSAGE =
        SELECT_ALL_FIELDS
            + """
              FROM $$inbox_message$$ message
              LEFT JOIN $$inbox_message_recipient$$ recipient ON recipient.inbox_message_fk = message.id
             WHERE message.id = ?
            """;

    @SuppressWarnings("java:S5665")
    private static final String SQL_UPDATE_INBOX_MESSAGE =
        """
        UPDATE $$inbox_message$$
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
        SELECT_VIEW_FIELDS
            + """
              FROM $$inbox_message$$ message
              JOIN $$inbox_message_recipient$$ recipient ON recipient.inbox_message_fk = message.id
             WHERE message.status NOT IN ('COMPLETED', 'ERROR')
             ORDER BY message.id ASC
            """;

    private static final String SQL_FIND_NEXT_PROCESSING_RELEVANT_BY_RECIPIENT_NAME =
        SELECT_VIEW_FIELDS
            + """
              FROM $$inbox_message$$ message
              JOIN $$inbox_message_recipient$$ recipient ON recipient.inbox_message_fk = message.id
             WHERE message.status NOT IN ('COMPLETED', 'ERROR')
               AND recipient.name IN ($$name_placeholders$$)
             ORDER BY message.id ASC
            """;

    private static final String SQL_FIND_FOR_RETRY =
        SELECT_VIEW_FIELDS
            + """
              FROM $$inbox_message$$ message
              JOIN $$inbox_message_recipient$$ recipient ON recipient.inbox_message_fk = message.id
             WHERE message.status = 'RETRY'
             ORDER BY message.retry_at ASC
            """;

    private static final String SQL_INSERT_INBOX_MESSAGE =
        """
        INSERT INTO $$inbox_message$$ (
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
        "INSERT INTO $$inbox_message_recipient$$ (name, inbox_message_fk) VALUES (?, ?)";

    private static final String PLACEHOLDER_INBOX_MESSAGE = "$$inbox_message$$";
    private static final String PLACEHOLDER_INBOX_MESSAGE_RECIPIENT = "$$inbox_message_recipient$$";

    private static final String DEFAULT_TABLE_NAME_INBOX_MESSAGE_RECIPIENT =
        "inbox_message_recipient";
    private static final String DEFAULT_TABLE_NAME_INBOX_MESSAGE = "inbox_message";

    private final String selectInboxMessage;
    private final String updateInboxMessage;
    private final String findAllProcessingRelevant;
    private final String findNextProcessingRelevantByRecipientName;
    private final String findForRetry;
    private final String insertInboxMessage;
    private final String insertInboxMessageRecipient;

    Sqls(String tableMessage, String tableRecipient) {
      tableMessage = tableMessage == null ? DEFAULT_TABLE_NAME_INBOX_MESSAGE : tableMessage;
      tableRecipient =
          tableRecipient == null ? DEFAULT_TABLE_NAME_INBOX_MESSAGE_RECIPIENT : tableRecipient;

      this.selectInboxMessage =
          replaceTablePlaceholders(SQL_SELECT_INBOX_MESSAGE, tableMessage, tableRecipient);
      this.updateInboxMessage =
          replaceTablePlaceholders(SQL_UPDATE_INBOX_MESSAGE, tableMessage, tableRecipient);
      this.findAllProcessingRelevant =
          replaceTablePlaceholders(SQL_FIND_ALL_PROCESSING_RELEVANT, tableMessage, tableRecipient);
      this.findNextProcessingRelevantByRecipientName =
          replaceTablePlaceholders(
              SQL_FIND_NEXT_PROCESSING_RELEVANT_BY_RECIPIENT_NAME, tableMessage, tableRecipient);
      this.findForRetry =
          replaceTablePlaceholders(SQL_FIND_FOR_RETRY, tableMessage, tableRecipient);
      this.insertInboxMessage =
          replaceTablePlaceholders(SQL_INSERT_INBOX_MESSAGE, tableMessage, tableRecipient);
      this.insertInboxMessageRecipient =
          replaceTablePlaceholders(
              SQL_INSERT_INBOX_MESSAGE_RECIPIENT, tableMessage, tableRecipient);
    }

    private static String replaceTablePlaceholders(
        String sql, String tableMessage, String tableRecipient) {
      return sql.replace(PLACEHOLDER_INBOX_MESSAGE, tableMessage)
          .replace(PLACEHOLDER_INBOX_MESSAGE_RECIPIENT, tableRecipient);
    }

    public String getSelectInboxMessage() {
      return selectInboxMessage;
    }

    public String getUpdateInboxMessage() {
      return updateInboxMessage;
    }

    public String getFindAllProcessingRelevant() {
      return findAllProcessingRelevant;
    }

    public String getFindNextProcessingRelevantByRecipientName() {
      return findNextProcessingRelevantByRecipientName;
    }

    public String getFindForRetry() {
      return findForRetry;
    }

    public String getInsertInboxMessage() {
      return insertInboxMessage;
    }

    public String getInsertInboxMessageRecipient() {
      return insertInboxMessageRecipient;
    }
  }

  static class Configuration {

    private final DataSource dataSource;
    private InstantSource instantSource = InstantSource.system();
    private OtelPlugin otelPlugin = new OtelPlugin();
    private String tableInboxMessage;
    private String tableInboxMessageRecipient;

    Configuration(DataSource dataSource) {
      this.dataSource = requireNonNull(dataSource);
    }

    public Configuration withInstantSource(InstantSource instantSource) {
      this.instantSource = instantSource;
      return this;
    }

    public Configuration withOtelPlugin(OtelPlugin otelPlugin) {
      this.otelPlugin = otelPlugin;
      return this;
    }

    public Configuration withTableInboxMessage(String tableInboxMessage) {
      this.tableInboxMessage = tableInboxMessage;
      return this;
    }

    public Configuration withTableInboxMessageRecipient(String tableInboxMessageRecipient) {
      this.tableInboxMessageRecipient = tableInboxMessageRecipient;
      return this;
    }
  }
}
