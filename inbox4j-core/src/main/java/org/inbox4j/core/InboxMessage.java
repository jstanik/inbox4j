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

import java.time.Instant;
import java.util.Set;

/** A message stored in {@link Inbox}. */
public interface InboxMessage {

  /** {@return the identifier of this message} */
  long getId();

  int getVersion();

  String getChannelName();

  Status getStatus();

  byte[] getPayload();

  byte[] getMetadata();

  Instant getRetryAt();

  /**
   * Gets the names that identify the recipient of this message.
   *
   * @return the set of recipient's names
   */
  Set<String> getRecipientNames();

  String getTraceContext();

  /** The status of the inbox message. */
  enum Status {
    NEW,
    IN_PROGRESS,
    RETRY,
    DELEGATED,
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
