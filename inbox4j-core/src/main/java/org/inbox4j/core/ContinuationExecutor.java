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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.inbox4j.core.ContinuationReferenceIssuer.IdVersion;

class ContinuationExecutor {
  private final ContinuationReferenceIssuer referenceIssuer;
  private final ExecutorService executor;
  private final OtelPlugin otelPlugin;

  ContinuationExecutor(
      ContinuationReferenceIssuer referenceIssuer,
      ExecutorService executor,
      OtelPlugin otelPlugin) {
    this.executor = executor;
    this.referenceIssuer = referenceIssuer;
    this.otelPlugin = otelPlugin;
  }

  CompletableFuture<Void> execute(InboxMessage inboxMessage, Continuation continuation) {
    return CompletableFuture.runAsync(
        () ->
            otelPlugin
                .getContextInstaller()
                .install(
                    inboxMessage.getTraceContext(),
                    () -> {
                      var continuationReference = referenceIssuer.issueReference(inboxMessage);
                      continuation.start(inboxMessage, continuationReference);
                    }),
        executor);
  }

  IdVersion dereference(ContinuationReference reference) {
    return referenceIssuer.dereference(reference);
  }
}
