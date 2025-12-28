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
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.LongConsumer;

class RetryScheduler extends ExecutorAwareLifecycle<ScheduledExecutorService> {

  private final InboxMessageRepository repository;
  private final InstantSource instantSource;

  RetryScheduler(
      ScheduledExecutorService executor,
      InboxMessageRepository repository,
      InstantSource instantSource) {
    super(executor);
    this.repository = repository;
    this.instantSource = instantSource;
  }

  /**
   * Schedules pending retries of stored messages.
   *
   * <p>This method should be used upon inbox initialization to pick up messages in RETRY status
   * already stored in the database.
   *
   * @param triggerConsumer the consumer of identifiers of messages whose retry delay has expired
   */
  void schedulePendingRetries(LongConsumer triggerConsumer) {
    Instant currentTime = instantSource.instant();
    repository
        .findInboxMessagesForRetry()
        .forEach(
            messageView -> {
              var delay = Duration.between(currentTime, messageView.retryAt());
              if (delay.isNegative()) {
                delay = Duration.ZERO;
              }
              scheduleRetry(messageView.id(), delay, triggerConsumer);
            });
  }

  /**
   * Schedules a retry with a given delay.
   *
   * @param messageId the identifier of the message
   * @param delay the delay after which the retry should be triggered
   * @param triggerConsumer the consumer of identifiers of messages whose retry delay has expired
   */
  void scheduleRetry(long messageId, Duration delay, LongConsumer triggerConsumer) {
    executor.schedule(
        () -> triggerConsumer.accept(messageId), delay.toMillis(), TimeUnit.MILLISECONDS);
  }
}
