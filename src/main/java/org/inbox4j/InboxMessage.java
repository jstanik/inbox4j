package org.inbox4j;

import java.util.Set;

public interface InboxMessage {
  long getId();

  int getVersion();

  String getChannelName();

  Status getStatus();

  byte[] getPayload();

  byte[] getMetadata();

  Set<String> getTargetNames();

  String getTraceContext();

  enum Status {
    NEW,
    IN_PROGRESS,
    ERROR,
    COMPLETED;

    public static Status forName(String name) {
      for (Status status : Status.values()) {
        if (status.name().equals(name)) {
          return status;
        }
      }

      throw new IllegalArgumentException("Unknown Status value: " + name);
    }
  }
}
