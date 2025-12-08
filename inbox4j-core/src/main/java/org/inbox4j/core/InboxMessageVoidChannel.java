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
 * A channel the inbox messages can be dispatched to. Unlike {@link InboxMessageChannel}, this
 * interface allows to rely on implicit result and its mapping to the {@link
 * org.inbox4j.core.InboxMessageChannel.ProcessingResult}.
 *
 * @see InboxMessageChannel
 */
public interface InboxMessageVoidChannel extends InboxMessageChannel {

  /**
   * Processes the inbox message.
   *
   * @param message the message to process.
   */
  void processMessageWithoutResult(InboxMessage message);

  @Override
  default ProcessingResult processMessage(InboxMessage message) {
    try {
      processMessageWithoutResult(message);
      return new ProcessingSucceededResult(message);
    } catch (Exception e) {
      return new ProcessingFailedResult(message, e);
    }
  }
}
