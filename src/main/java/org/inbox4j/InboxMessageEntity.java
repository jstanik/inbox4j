package org.inbox4j;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class InboxMessageEntity implements InboxMessage {

  private final long id;
  private final int version;
  private final Instant createdAt;
  private final Instant updatedAt;
  private final Set<String> targetNames;
  private final String channelName;
  private final Status status;
  private final byte[] payload;
  private final byte[] metadata;
  private final String traceContext;
  private final String auditLog;

  InboxMessageEntity(Builder builder) {
    this.id = builder.id;
    this.version = builder.version;
    this.createdAt = builder.createdAt;
    this.updatedAt = builder.updatedAt;
    this.targetNames = Set.copyOf(builder.targetNames);
    this.channelName = builder.channelName;
    this.status = builder.status;
    this.payload = builder.payload;
    this.metadata = builder.metadata;
    this.traceContext = builder.traceContext;
    this.auditLog = builder.auditLog;
  }

  public long getId() {
    return id;
  }

  public int getVersion() {
    return version;
  }

  public Instant getCreatedAt() {
    return createdAt;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  @Override
  public String getChannelName() {
    return channelName;
  }

  @Override
  public Status getStatus() {
    return status;
  }

  @Override
  public byte[] getPayload() {
    return payload;
  }

  @Override
  public byte[] getMetadata() {
    return metadata;
  }

  @Override
  public Set<String> getTargetNames() {
    return targetNames;
  }

  @Override
  public String getTraceContext() {
    return traceContext;
  }

  public String getAuditLog() {
    return auditLog;
  }

  static class Builder {

    private long id;
    private int version;
    private Instant createdAt;
    private Instant updatedAt;
    private final List<String> targetNames = new ArrayList<>();
    private String channelName;
    private Status status;
    private byte[] payload;
    private byte[] metadata;
    private String traceContext;
    private String auditLog;

    public long getId() {
      return id;
    }

    public Builder setId(long id) {
      this.id = id;
      return this;
    }

    public Builder setVersion(int version) {
      this.version = version;
      return this;
    }

    public Builder setCreatedAt(Instant createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder setUpdatedAt(Instant updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public Builder addTargetName(String targetName) {
      if (targetName != null) {
        this.targetNames.add(targetName);
      }
      return this;
    }

    public Builder setChannelName(String channelName) {
      this.channelName = channelName;
      return this;
    }

    public Builder setStatus(Status status) {
      this.status = status;
      return this;
    }

    public Builder setPayload(byte[] payload) {
      this.payload = payload;
      return this;
    }

    public Builder setMetadata(byte[] metadata) {
      this.metadata = metadata;
      return this;
    }

    public Builder setTraceContext(String traceContext) {
      this.traceContext = traceContext;
      return this;
    }

    public Builder setAuditLog(String auditLog) {
      this.auditLog = auditLog;
      return this;
    }

    public InboxMessageEntity createInboxMessageEntity() {
      return new InboxMessageEntity(this);
    }
  }
}