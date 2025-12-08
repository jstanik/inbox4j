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

abstract class AbstractProcessingResult {
  private final InboxMessage inboxMessage;
  private final byte[] metadata;

  protected AbstractProcessingResult(InboxMessage inboxMessage, byte[] metadata) {
    this.inboxMessage = inboxMessage;
    this.metadata = metadata;
  }

  public InboxMessage getInboxMessage() {
    return this.inboxMessage;
  }

  public byte[] getMetadata() {
    return this.metadata;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "{inboxMessageId="
        + getInboxMessage().getId()
        + ", recipientNames="
        + getInboxMessage().getRecipientNames()
        + "}";
  }
}
