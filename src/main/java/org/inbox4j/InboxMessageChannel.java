package org.inbox4j;

public interface InboxMessageChannel {

  String getName();

  void processMessage(InboxMessage message);

}
