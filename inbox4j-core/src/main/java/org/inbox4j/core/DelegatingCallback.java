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

/**
 * A callback for initiating delegated processing.
 *
 * <p>Implementations of this interface are responsible for executing the asynchronous processing of
 * an inbox message.
 */
public interface DelegatingCallback {

  /**
   * Initiates asynchronous delegated processing for the given message.
   *
   * <p>This method must return promptly and must not block until the processing has completed.
   * Implementations are responsible for handling all exceptions internally and must report
   * completion (successful or failed) using the provided {@code reference} via the {@link Inbox}
   * interface.
   *
   * @param message the message the processing of which to delegate
   * @param reference the reference identifying this delegated processing
   */
  void delegate(InboxMessage message, DelegationReference reference);
}
