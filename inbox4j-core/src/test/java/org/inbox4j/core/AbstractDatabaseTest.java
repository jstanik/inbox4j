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

import static org.inbox4j.core.TransactionTemplate.transactionWithoutResult;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;

abstract class AbstractDatabaseTest {

  static final OtelPlugin otelPlugin = new OtelPlugin();
  static final DataSource dataSource = postgresDataSource();

  protected static DataSource postgresDataSource() {
    return postgresDataSource(
        "jdbc:tc:postgresql:18.1:///postgres?TC_INITSCRIPT=file:src/main/sql/postgresql-ddl.sql");
  }

  protected static DataSource postgresDataSource(String connectionUrl) {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(connectionUrl);
    config.setUsername("postgres");
    config.setPassword("postgres");
    config.setAutoCommit(false);
    config.setMaximumPoolSize(1); // keep it 1 to detect unclosed connections early
    return new HikariDataSource(config);
  }

  @BeforeEach
  void beforeEach() {
    clearInboxMessageTable();
  }

  private static void clearInboxMessageTable() {
    try (var connection = dataSource.getConnection()) {
      transactionWithoutResult(
          connection,
          c -> {
            try (var statement = c.prepareStatement("DELETE FROM inbox_message")) {
              statement.execute();
            }
          });
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
