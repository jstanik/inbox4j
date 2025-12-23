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

import static org.assertj.core.api.Assertions.assertThat;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.inbox4j.core.InboxMessage.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ContinuationExecutorTest {

  ExecutorService executor = Executors.newSingleThreadExecutor();

  @AfterEach
  void tearDown() {
    executor.shutdownNow();
  }

  @Test
  void execute() {
    ContinuationExecutor cut =
        new ContinuationExecutor(new ContinuationReferenceIssuer(), executor, new OtelPlugin());
    InboxMessage inboxMessage =
        new InboxMessageEntity.Builder()
            .setId(1)
            .setVersion(4)
            .setStatus(Status.WAITING_FOR_CONTINUATION)
            .createInboxMessageEntity();

    var actualTraceContext = new AtomicReference<String>();
    Continuation continuation =
        (m, reference) -> actualTraceContext.set(Span.current().getSpanContext().getTraceId());

    Span span =
        GlobalOpenTelemetry.getTracer(InboxMessageRepositoryTest.class.getName())
            .spanBuilder("continuation")
            .setParent(Context.root())
            .startSpan();

    String expectedTraceId;
    CompletableFuture<Void> actual;
    try (Scope ignore = span.makeCurrent()) {
      expectedTraceId = span.getSpanContext().getTraceId();
      actual = cut.execute(inboxMessage, continuation);
    } finally {
      span.end();
    }

    assertThat(actual).succeedsWithin(Duration.ofSeconds(60));
    assertThat(actualTraceContext.get()).isEqualTo(expectedTraceId);
  }
}
