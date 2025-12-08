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

/**
 * A result indicating that the message processing should ot continue inline. Instead, the caller is
 * expected to delegate the processing of the inbox message to the callback contained in this
 * result.
 */
public final class DelegateResult extends AbstractProcessingResult implements ProcessingResult {

  private final DelegatingCallback delegatingCallback;

  /**
   * Construct a {@code DelegateResult} that requests the delegation of the given inbox message to
   * the supplied callback.
   *
   * @param inboxMessage the inbox message whose processing is to be delegated
   * @param delegatingCallback the callback to which processing should be delegated
   */
  public DelegateResult(InboxMessage inboxMessage, DelegatingCallback delegatingCallback) {
    this(inboxMessage, delegatingCallback, inboxMessage.getMetadata());
  }

  /**
   * Construct a {@code DelegateResult} that requests the delegation of the given inbox message to
   * the supplied callback.
   *
   * @param inboxMessage the inbox message whose processing is to be delegated
   * @param delegatingCallback the callback to which processing should be delegated
   * @param metadata the metadata to store in the inbox message before invoking the callback
   */
  public DelegateResult(
      InboxMessage inboxMessage, DelegatingCallback delegatingCallback, byte[] metadata) {
    super(inboxMessage, metadata);
    this.delegatingCallback = delegatingCallback;
  }

  public DelegatingCallback getDelegatingCallback() {
    return delegatingCallback;
  }
}
