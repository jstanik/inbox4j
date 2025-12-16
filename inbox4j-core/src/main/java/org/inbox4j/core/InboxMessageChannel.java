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

import java.time.Duration;

/**
 * A channel the inbox messages can be dispatched to.
 *
 * <p>The channel is responsible for processing the inbox message.
 *
 * @see InboxMessageVoidChannel
 */
public interface InboxMessageChannel {

  /** {@return the name of this channel} */
  String getName();

  /**
   * Processes the inbox message.
   *
   * @param message the inbox message to process
   * @return the result of the processing
   */
  ProcessingResult processMessage(InboxMessage message);

  /** The result of the inbox message processing. */
  sealed interface ProcessingResult
      permits DelegateResult, ProcessingFailedResult, ProcessingSucceededResult, RetryResult {}

  /** A successful result. */
  final class ProcessingSucceededResult extends AbstractProcessingResult
      implements ProcessingResult {

    public ProcessingSucceededResult(InboxMessage inboxMessage) {
      super(inboxMessage);
    }

    public ProcessingSucceededResult(InboxMessage inboxMessage, byte[] metadata) {
      super(inboxMessage, metadata);
    }
  }

  /** A failed result. */
  final class ProcessingFailedResult extends AbstractProcessingResult implements ProcessingResult {

    private final Throwable error;

    public ProcessingFailedResult(InboxMessage inboxMessage, Throwable error) {
      this(inboxMessage, inboxMessage.getMetadata(), error);
    }

    public ProcessingFailedResult(InboxMessage inboxMessage, byte[] metadata, Throwable error) {
      super(inboxMessage, metadata);
      this.error = error;
    }

    /**
     * Gets the error that caused the processing failure.
     *
     * @return the error
     */
    public Throwable getError() {
      return this.error;
    }
  }

  /**
   * A result indicating that processing should be retried after the specified delay. The inbox
   * guarantees only that the retry will not occur before this delay; it does not guarantee the
   * exact timing of the retry. Retry requests are handled on a best-effort basis with respect to
   * the requested delay.
   */
  final class RetryResult extends AbstractProcessingResult implements ProcessingResult {

    private final Duration delay;

    /**
     * Creates new result representing retry request.
     *
     * @param inboxMessage the inbox message
     * @param delay the retry delay
     */
    public RetryResult(InboxMessage inboxMessage, Duration delay) {
      this(inboxMessage, inboxMessage.getMetadata(), delay);
    }

    /**
     * Creates new result representing retry request.
     *
     * @param inboxMessage the inbox message
     * @param metadata the new metadata to attach to the inbox message
     * @param delay the retry delay
     */
    public RetryResult(InboxMessage inboxMessage, byte[] metadata, Duration delay) {
      super(inboxMessage, metadata);
      if (delay.isNegative()) {
        throw new IllegalArgumentException("delay cannot be negative");
      }
      this.delay = delay;
    }

    /**
     * Gets the delay after which the processing should be retried.
     *
     * @return the delay
     */
    public Duration getDelay() {
      return delay;
    }
  }
}
