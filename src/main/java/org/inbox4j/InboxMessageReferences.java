package org.inbox4j;

import java.util.ArrayList;
import java.util.List;
import org.inbox4j.InboxMessage.Status;

record InboxMessageReferences(long id, Names targetNames) {

  static class Builder {

    final long id;
    final Status status;
    final List<String> names = new ArrayList<>();

    Builder(long id, Status status) {
      this.id = id;
      this.status = status;
    }

    Builder addName(String name) {
      this.names.add(name);
      return this;
    }

    InboxMessageReferences build() {
      return new InboxMessageReferences(
          id,
          new Names(names)
      );
    }
  }
}
