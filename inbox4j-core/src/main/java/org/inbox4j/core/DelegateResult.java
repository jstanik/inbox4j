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

public final class DelegateResult extends AbstractProcessingResult implements ProcessingResult {

  private final DelegatingCallback delegatingCallback;

  public DelegateResult(InboxMessage inboxMessage, DelegatingCallback delegatingCallback) {
    this(inboxMessage, delegatingCallback, inboxMessage.getMetadata());
  }

  public DelegateResult(
      InboxMessage inboxMessage, DelegatingCallback delegatingCallback, byte[] metadata) {
    super(inboxMessage, metadata);
    this.delegatingCallback = delegatingCallback;
  }

  public DelegatingCallback getDelegatingCallback() {
    return delegatingCallback;
  }
}
