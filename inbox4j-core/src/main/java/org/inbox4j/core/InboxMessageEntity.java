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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

class InboxMessageEntity implements InboxMessage {

  private final long id;
  private final int version;
  private final Instant createdAt;
  private final Instant updatedAt;
  private final Set<String> recipientNames;
  private final String channelName;
  private final Status status;
  private final byte[] payload;
  private final byte[] metadata;
  private final Instant retryAt;
  private final String traceContext;
  private final String auditLog;

  InboxMessageEntity(Builder builder) {
    this.id = builder.id;
    this.version = builder.version;
    this.createdAt = builder.createdAt;
    this.updatedAt = builder.updatedAt;
    this.recipientNames = Set.copyOf(builder.recipientNames);
    this.channelName = builder.channelName;
    this.status = builder.status;
    this.payload = builder.payload;
    this.metadata = builder.metadata;
    this.retryAt = builder.retryAt;
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
  public Instant getRetryAt() {
    return retryAt;
  }

  @Override
  public Set<String> getRecipientNames() {
    return recipientNames;
  }

  @Override
  public String getTraceContext() {
    return traceContext;
  }

  public String getAuditLog() {
    return auditLog;
  }

  @Override
  public String toString() {
    return "InboxMessageEntity{"
        + "id="
        + id
        + ", version="
        + version
        + ", status="
        + status
        + ", recipientNames="
        + recipientNames
        + ", retryAt="
        + retryAt
        + '}';
  }

  static class Builder {

    private long id;
    private int version;
    private Instant createdAt;
    private Instant updatedAt;
    private final List<String> recipientNames = new ArrayList<>();
    private String channelName;
    private Status status;
    private byte[] payload;
    private byte[] metadata;
    private Instant retryAt;
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

    public Builder addRecipientName(String recipientName) {
      if (recipientName != null) {
        this.recipientNames.add(recipientName);
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

    public Builder setRetryAt(Instant retryAt) {
      this.retryAt = retryAt;
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
