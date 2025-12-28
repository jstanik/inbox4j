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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.InstantSource;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.inbox4j.core.InboxMessage.Status;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class RetrySchedulerTest {
  @Test
  void schedulePendingRetries() {
    var executor = mock(ScheduledExecutorService.class);
    var repository = mock(InboxMessageRepository.class);
    var instantReference = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    var instantSource = InstantSource.fixed(instantReference);
    var cut = new RetryScheduler(executor, repository, instantSource);

    var retryInPast = view(1, instantReference.minusMillis(1));
    var retryInFuture = view(2, instantReference.plusMillis(60000));
    when(repository.findInboxMessagesForRetry()).thenReturn(List.of(retryInPast, retryInFuture));

    List<Long> triggeredIds = new ArrayList<>();
    cut.schedulePendingRetries(triggeredIds::add);

    var captor = ArgumentCaptor.forClass(Runnable.class);

    verify(executor).schedule(captor.capture(), eq(0L), eq(TimeUnit.MILLISECONDS));
    captor.getValue().run();
    verify(executor).schedule(captor.capture(), eq(60000L), eq(TimeUnit.MILLISECONDS));
    captor.getValue().run();

    Assertions.assertThat(triggeredIds).containsExactly(1L, 2L);
  }

  private static InboxMessageView view(long id, Instant retryAt) {
    return new InboxMessageView(id, Status.RETRY, retryAt, new Recipient(List.of()));
  }
}
