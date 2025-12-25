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

class RetentionPolicy extends ExecutorAwareLifecycle<ScheduledExecutorService> {

  private static final Duration MINIMAL_RETENTION_PERIOD = java.time.Duration.ofMinutes(1);

  private final Duration retentionPeriod;
  private final InboxMessageRepository repository;
  private final InstantSource instantSource;

  RetentionPolicy(
      Duration retentionPeriod,
      InboxMessageRepository repository,
      ScheduledExecutorService executor,
      InstantSource instantSource) {
    super(executor);
    this.retentionPeriod = validate(retentionPeriod);
    this.repository = repository;
    this.instantSource = instantSource;
  }

  private static Duration validate(Duration retentionPeriod) {
    if (retentionPeriod.compareTo(MINIMAL_RETENTION_PERIOD) < 0) {
      throw new IllegalArgumentException("timeRetention must be >= " + MINIMAL_RETENTION_PERIOD);
    }
    return retentionPeriod;
  }

  void apply() {
    Duration defaultCheckPeriod = Duration.ofDays(1);
    Duration checkPeriod =
        retentionPeriod.compareTo(defaultCheckPeriod) < 0 ? retentionPeriod : defaultCheckPeriod;
    executor.scheduleAtFixedRate(this::cleanup, 0, checkPeriod.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void cleanup() {
    Instant instantThreshold = instantSource.instant().minus(retentionPeriod);
    repository.deleteInboxMessagesCompletedBefore(instantThreshold);
  }
}
