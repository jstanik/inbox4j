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

import javax.sql.DataSource;
import org.inbox4j.core.InboxMessage.Status;

/**
 * An inbox for incoming messages.
 *
 * <p>Messages in the inbox are dispatched to {@link InboxMessageChannel message channels} for later
 * processing. The inbox ensures that messages for the same recipient are dispatched sequentially,
 * one at a time until their processing is completed, while messages for different recipients may be
 * dispatched in parallel.
 *
 * <p>Use {@link Inbox#builder(DataSource)} to obtain new instances of Inbox.
 *
 * @see InboxMessage
 * @see InboxMessageChannel
 */
public interface Inbox extends AutoCloseable {

  /** {@return a new builder to create a new inbox instance} */
  static InboxBuilder builder(DataSource dataSource) {
    return new InboxBuilder(dataSource);
  }

  /**
   * Inserts a new message to the inbox according to the request. This method returns immediately
   * after the message is inserted in inbox. It <b>does not</b> wait for the message to be
   * dispatched to {@link InboxMessageChannel}.
   *
   * @param request describing the content of the inbox message
   * @return the inserted inbox message
   */
  InboxMessage insert(MessageInsertionRequest request);

  /**
   * Loads a message by its identifier.
   *
   * @param id the message identifier
   * @return the loaded message
   */
  InboxMessage load(long id);

  /**
   * Completes the processing of an inbox message.
   *
   * @param continuationReference the reference to the continuation that started the processing
   * @param success flag signaling success or failure of the operation
   */
  void complete(ContinuationReference continuationReference, boolean success);

  /**
   * Resets an inbox message to the {@link Status#NEW} status. Reset is only allowed if the inbox
   * message is in {@link Status#ERROR} status.
   *
   * @param id the identifier of the inbox message to reset
   * @throws IllegalStateException if the inbox message is not in the {@link Status#ERROR}
   */
  void reset(long id);

  /**
   * Removes the inbox message from the inbox.
   *
   * <p>Use this method with care! It removes the inbox message regardless of its {@link Status}.
   * This method is intended to be used primarily for messages which ended in {@link Status#ERROR}
   * status by mistake or are incorrect, and by being in the {@link Status#ERROR} they block
   * processing of other messages for the same recipient.
   *
   * @param id the identifier of the inbox message to remove
   */
  void remove(long id);
}
