package org.inbox4j;

import static org.inbox4j.TransactionTemplate.transactionWithoutResult;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;

public abstract class AbstractDatabaseTest {

  protected static final DataSource dataSource = postgresDataSource();

  protected static DataSource postgresDataSource() {
    HikariConfig config = new HikariConfig();

    config.setJdbcUrl(
        "jdbc:tc:postgresql:18.1:///postgres?TC_INITSCRIPT=file:src/main/sql/postgresql-ddl.sql");
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
      transactionWithoutResult(connection, c -> {
        try (var statement = c.prepareStatement("DELETE FROM inbox_message")) {
          statement.execute();
        }
      });
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
