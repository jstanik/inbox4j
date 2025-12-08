package org.inbox4j;

import java.util.Collection;
import java.util.Set;

public class InboxMessageData {

  private final String channelName;
  private final byte[] payload;
  private final byte[] metadata;
  private final Set<String> targetNames;

  public InboxMessageData(String channelName, byte[] payload, String targetName) {
    this(channelName, payload, Set.of(targetName), null);
  }

  public InboxMessageData(String channelName, byte[] payload, String targetName, byte[] metadata) {
    this(channelName, payload, Set.of(targetName), metadata);
  }

  public InboxMessageData(String channelName, byte[] payload, Collection<String> targetNames,
      byte[] metadata) {
    this.channelName = channelName;
    this.payload = payload;
    this.metadata = metadata;
    this.targetNames = Set.copyOf(targetNames);
  }

  String getChannelName() {
    return channelName;
  }

  byte[] getPayload() {
    return payload;
  }

  public Set<String> getTargetNames() {
    return targetNames;
  }

  byte[] getMetadata() {
    return metadata;
  }
}
