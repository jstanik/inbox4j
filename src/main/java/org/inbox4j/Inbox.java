package org.inbox4j;

import javax.sql.DataSource;

public interface Inbox {

  static InboxBuilder create(DataSource dataSource) {
    return new InboxBuilder(dataSource);
  }

  InboxMessage insert(InboxMessageData data);

  InboxMessage load(long id);
}

