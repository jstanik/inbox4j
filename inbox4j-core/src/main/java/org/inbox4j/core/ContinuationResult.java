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

import org.inbox4j.core.InboxMessageChannel.ProcessingResult;

/** A result indicating that the message processing should continue outside of inbox. */
public final class ContinuationResult extends AbstractProcessingResult implements ProcessingResult {

  private final Continuation continuation;

  /**
   * Constructs a {@code ContinuationResult} that requests the processing to continue outside the
   * inbox.
   *
   * @param inboxMessage the inbox message whose processing is to be continued
   * @param continuation the callback which should start the continued processing
   */
  public ContinuationResult(InboxMessage inboxMessage, Continuation continuation) {
    this(inboxMessage, continuation, inboxMessage.getMetadata());
  }

  /**
   * Constructs a {@code ContinuationResult} that requests the processing to continue outside the
   * inbox.
   *
   * @param inboxMessage the inbox message whose processing is to be continued
   * @param continuation the callback which should start the continued processing
   * @param metadata the metadata to store in the inbox message before invoking the continuation
   */
  public ContinuationResult(InboxMessage inboxMessage, Continuation continuation, byte[] metadata) {
    super(inboxMessage, metadata);
    this.continuation = continuation;
  }

  public Continuation getContinuation() {
    return continuation;
  }
}
