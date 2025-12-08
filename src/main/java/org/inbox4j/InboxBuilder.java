package org.inbox4j;

import static java.util.Objects.requireNonNull;

import java.time.InstantSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

public class InboxBuilder {

  private final DataSource dataSource;
  private final List<InboxMessageChannel> channels = new ArrayList<>();
  private ExecutorService executorService;
  private int maxConcurrency;

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

  public Inbox build() {
    var repository = new InboxMessageRepository(dataSource, InstantSource.system());

    ExecutorService configuredExecutorService = executorService;
    if (configuredExecutorService == null) {
      configuredExecutorService = Executors.newCachedThreadPool();
    }
    return new ProcessingInbox(repository, channels, configuredExecutorService, maxConcurrency);
  }
}
