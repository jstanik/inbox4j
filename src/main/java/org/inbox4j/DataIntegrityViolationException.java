package org.inbox4j;

public class DataIntegrityViolationException extends DatabaseAccessException {

  public DataIntegrityViolationException(String message, Throwable cause) {
    super(message, cause);
  }
}
