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

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TransactionTemplate {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionTemplate.class);

  private TransactionTemplate() {}

  @FunctionalInterface
  public interface TransactionCallback<T> {

    T doInTransaction(Connection connection) throws SQLException;
  }

  @FunctionalInterface
  public interface TransactionWithoutResultCallback {

    void doInTransaction(Connection connection) throws SQLException;
  }

  public static void transactionWithoutResult(
      DataSource dataSource, TransactionWithoutResultCallback callback) {
    try (Connection c = dataSource.getConnection()) {
      transactionWithoutResult(c, callback);
    } catch (SQLException e) {
      throw new DatabaseAccessException("Database access failure", e);
    }
  }

  public static void transactionWithoutResult(
      Connection connection, TransactionWithoutResultCallback callback) {
    transaction(
        connection,
        c -> {
          callback.doInTransaction(c);
          return null;
        });
  }

  public static <T> T transaction(DataSource dataSource, TransactionCallback<T> callback) {
    try (Connection c = dataSource.getConnection()) {
      return transaction(c, callback);
    } catch (SQLException e) {
      throw new DatabaseAccessException("Database access failure", e);
    }
  }

  public static <T> T transaction(Connection connection, TransactionCallback<T> callback) {
    boolean initialAutoCommit = false;
    Throwable exceptionFromTry = null;
    try {
      initialAutoCommit = connection.getAutoCommit();
      if (initialAutoCommit) {
        connection.setAutoCommit(false);
      }
      T result = callback.doInTransaction(connection);
      connection.commit();
      return result;
    } catch (Exception e) {
      exceptionFromTry = e;
      try {
        connection.rollback();
      } catch (SQLException rollbackEx) {
        e.addSuppressed(rollbackEx);
      }
      throw translateException(e);
    } finally {
      try {
        if (initialAutoCommit) {
          connection.setAutoCommit(true);
        }
      } catch (SQLException resetEx) {
        if (exceptionFromTry != null) {
          exceptionFromTry.addSuppressed(resetEx);
        } else {
          LOGGER.error("Failed to restore auto-commit flag on the connection", resetEx);
        }
      }
    }
  }

  private static RuntimeException translateException(Exception exception) {
    if (exception instanceof DatabaseAccessException databaseAccessException) {
      throw databaseAccessException;
    }
    if (exception instanceof SQLException sqlException) {
      if (isDataIntegrityViolationException(sqlException)) {
        return new DataIntegrityViolationException(sqlException.getMessage(), sqlException);
      }
      return new DatabaseAccessException("Database access error", exception);
    }
    if (exception instanceof RuntimeException runtimeException) {
      return runtimeException;
    } else {
      return new IllegalStateException("Unexpected error occurred", exception);
    }
  }

  private static boolean isDataIntegrityViolationException(SQLException sqlException) {
    return getSqlState(sqlException).startsWith("23");
  }

  private static String getSqlState(SQLException exception) {
    var state = exception.getSQLState();
    return state == null ? "" : state;
  }
}
