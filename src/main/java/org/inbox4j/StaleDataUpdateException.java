package org.inbox4j;

public class StaleDataUpdateException extends DatabaseAccessException {

  public StaleDataUpdateException(String message, Throwable cause) {
    super(message, cause);
  }

  public StaleDataUpdateException(String message) {
    super(message);
  }
}
