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

import java.util.Collection;
import java.util.Set;

/** A request to submit a new message to the {@link Inbox}.¬ */
public class SubmitInboxMessageRequest {

  private final String channelName;
  private final byte[] payload;
  private final byte[] metadata;
  private final Set<String> recipientNames;

  /**
   * Creates a new request.
   *
   * @param channelName the name of the channel the submitted message should be dispatched to
   * @param payload the payload of the message
   * @param recipientName the name of the message recipient
   */
  public SubmitInboxMessageRequest(String channelName, byte[] payload, String recipientName) {
    this(channelName, payload, Set.of(recipientName), null);
  }

  /**
   * Creates a new request.
   *
   * @param channelName the name of the channel the submitted message should be dispatched to
   * @param payload the payload of the message
   * @param recipientName the name of the message recipient
   * @param metadata the metadata that will be attached to the submitted inbox message
   */
  public SubmitInboxMessageRequest(
      String channelName, byte[] payload, String recipientName, byte[] metadata) {
    this(channelName, payload, Set.of(recipientName), metadata);
  }

  /**
   * Creates a new request.
   *
   * @param channelName the name of the channel the submitted message should be dispatched to
   * @param payload the payload of the message
   * @param recipientNames the various names of the recipient
   * @param metadata the metadata that will be attached to the submitted inbox message
   */
  public SubmitInboxMessageRequest(
      String channelName, byte[] payload, Collection<String> recipientNames, byte[] metadata) {
    this.channelName = channelName;
    this.payload = payload;
    this.metadata = metadata;
    this.recipientNames = Set.copyOf(recipientNames);
  }

  String getChannelName() {
    return channelName;
  }

  byte[] getPayload() {
    return payload;
  }

  public Set<String> getRecipientNames() {
    return recipientNames;
  }

  byte[] getMetadata() {
    return metadata;
  }
}
