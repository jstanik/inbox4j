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

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import javax.sql.DataSource;
import org.inbox4j.core.InboxMessageRepository.Configuration;

public class InboxBuilder {

  private static final OtelPlugin OTEL_PLUGIN = new OtelPlugin();

  private final DataSource dataSource;
  private final List<InboxMessageChannel> channels = new ArrayList<>();
  private ExecutorService executorService;
  private ScheduledExecutorService internalExecutorService;
  private ExecutorService eventLoopExecutor;
  private int maxConcurrency = Integer.MAX_VALUE;
  private InstantSource instantSource = InstantSource.system();
  private String tableInboxMessage;
  private String tableInboxMessageRecipient;
  private Duration retentionPeriod = Duration.ofDays(7);

  public InboxBuilder(DataSource dataSource) {
    this.dataSource = requireNonNull(dataSource);
  }

  public InboxBuilder addChannel(InboxMessageChannel channel) {
    this.channels.add(channel);
    return this;
  }

  public InboxBuilder addChannels(Collection<InboxMessageChannel> channels) {
    this.channels.addAll(channels);
    return this;
  }

  public InboxBuilder withExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
    return this;
  }

  public InboxBuilder withMaxConcurrency(int maxConcurrency) {
    this.maxConcurrency = maxConcurrency;
    return this;
  }

  public InboxBuilder withTableInboxMessage(String tableInboxMessage) {
    this.tableInboxMessage = tableInboxMessage;
    return this;
  }

  public InboxBuilder withTableInboxMessageRecipient(String tableInboxMessageRecipient) {
    this.tableInboxMessageRecipient = tableInboxMessageRecipient;
    return this;
  }

  public InboxBuilder withRetentionPeriod(Duration retentionPeriod) {
    this.retentionPeriod = retentionPeriod;
    return this;
  }

  InboxBuilder withInstantSource(InstantSource instantSource) {
    this.instantSource = instantSource;
    return this;
  }

  InboxBuilder withInternalExecutorService(ScheduledExecutorService executorService) {
    this.internalExecutorService = executorService;
    return this;
  }

  InboxBuilder withEventLoopExecutorService(ExecutorService executorService) {
    this.eventLoopExecutor = executorService;
    return this;
  }

  private static ExecutorService ensureExecutorService(ExecutorService executorService) {
    return executorService != null ? executorService : Executors.newCachedThreadPool();
  }

  private static ExecutorService ensureEventLoopExecutorService(ExecutorService executorService) {
    return executorService != null
        ? executorService
        : Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "inbox-event-loop"));
  }

  private static ScheduledExecutorService ensureInternalExecutorService(
      ScheduledExecutorService executorService) {
    return executorService != null ? executorService : Executors.newSingleThreadScheduledExecutor();
  }

  public Inbox build() {
    var repository =
        new InboxMessageRepository(
            new Configuration(dataSource)
                .withInstantSource(instantSource)
                .withOtelPlugin(OTEL_PLUGIN)
                .withTableInboxMessage(tableInboxMessage)
                .withTableInboxMessageRecipient(tableInboxMessageRecipient));

    var resolvedExecutorService = ensureExecutorService(this.executorService);
    var resolvedInternalExecutorService =
        ensureInternalExecutorService(this.internalExecutorService);
    var resolvedEventLoopExecutorService = ensureEventLoopExecutorService(this.eventLoopExecutor);

    var dispatcher = new Dispatcher(channels, resolvedExecutorService, OTEL_PLUGIN);
    var continuationExecutor =
        new ContinuationExecutor(
            new ContinuationReferenceIssuer(), resolvedExecutorService, OTEL_PLUGIN);
    var retryManager =
        new RetryScheduler(resolvedInternalExecutorService, repository, instantSource);
    var retentionPolicy =
        new RetentionPolicy(
            retentionPeriod, repository, resolvedInternalExecutorService, instantSource);

    return new InboxController(
        repository,
        dispatcher,
        continuationExecutor,
        retryManager,
        retentionPolicy,
        resolvedEventLoopExecutorService,
        maxConcurrency,
        instantSource);
  }

  Inbox buildAndStart() {
    var inbox = build();
    inbox.start();
    return inbox;
  }
}
