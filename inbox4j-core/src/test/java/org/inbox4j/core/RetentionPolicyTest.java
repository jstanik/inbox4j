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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RetentionPolicyTest {

  @Test
  void constructorFailsIfRetentionPriodBelowAllowedLimit() {
    var expectedMinimalRetention = Duration.ofMinutes(1);
    var retentionPeriod = expectedMinimalRetention.minusMillis(1);

    assertThatThrownBy(() -> new RetentionPolicy(retentionPeriod, null, null, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("timeRetention must be >= " + expectedMinimalRetention);
  }

  @Test
  void apply() {
    var retentionPeriod = Duration.ofDays(7);
    var instantSource = InstantSource.fixed(Instant.now());
    var repository = mock(InboxMessageRepository.class);
    var scheduledExecutorService = mock(ScheduledExecutorService.class);
    var cut =
        new RetentionPolicy(retentionPeriod, repository, scheduledExecutorService, instantSource);

    cut.apply();

    var captor = ArgumentCaptor.forClass(Runnable.class);
    verify(scheduledExecutorService)
        .scheduleAtFixedRate(
            captor.capture(), eq(0L), eq(Duration.ofDays(1).toMillis()), eq(TimeUnit.MILLISECONDS));
    captor.getValue().run();

    var expectedThreshold = instantSource.instant().minus(retentionPeriod);
    verify(repository).deleteInboxMessagesCompletedBefore(expectedThreshold);
  }

  @Test
  void applySchedulesRetryMoreOftenIfRetentionLessThanADay() {
    var retentionPeriod = Duration.ofHours(3);
    var instantSource = InstantSource.fixed(Instant.now());
    var scheduledExecutorService = mock(ScheduledExecutorService.class);
    var cut = new RetentionPolicy(retentionPeriod, null, scheduledExecutorService, instantSource);

    cut.apply();

    verify(scheduledExecutorService)
        .scheduleAtFixedRate(
            any(Runnable.class), eq(0L), eq(retentionPeriod.toMillis()), eq(TimeUnit.MILLISECONDS));
  }
}
