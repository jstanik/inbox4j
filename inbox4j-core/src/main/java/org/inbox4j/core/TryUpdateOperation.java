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
import java.util.function.Consumer;
import org.inbox4j.core.InboxMessage.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TryUpdateOperation implements UpdateStatusSpec, UpdateMetadataSpec, UpdateRetryAtSpec {

  private static final Logger LOGGER = LoggerFactory.getLogger(TryUpdateOperation.class);

  private final InboxMessage inboxMessage;
  private final InboxMessageRepository repository;
  private Status status;
  private byte[] metadata;
  private Instant retryAt;

  TryUpdateOperation(InboxMessage inboxMessage, InboxMessageRepository repository) {
    this.inboxMessage = inboxMessage;
    this.repository = repository;
  }

  @Override
  public UpdateRetryAtSpec metadata(byte[] metadata) {
    this.metadata = metadata;
    return this;
  }

  @Override
  public UpdateExecutor retryAt(Instant retryAt) {
    this.retryAt = retryAt;
    return this;
  }

  @Override
  public UpdateMetadataSpec status(Status status) {
    this.status = status;
    return this;
  }

  @Override
  public InboxMessage execute() {
    return execute(m -> {});
  }

  @Override
  public InboxMessage execute(Consumer<InboxMessage> postUpdate) {
    try {
      InboxMessage updatedMessage = repository.update(inboxMessage, status, metadata, retryAt);
      postUpdate.accept(updatedMessage);
      return updatedMessage;
    } catch (Exception exception) {
      LOGGER.error("Update of InboxMessage{id={}} failed", inboxMessage.getId(), exception);
      return inboxMessage;
    }
  }
}

interface UpdateStatusSpec {
  UpdateMetadataSpec status(Status status);
}

interface UpdateMetadataSpec {
  UpdateRetryAtSpec metadata(byte[] metadata);
}

interface UpdateExecutor {
  InboxMessage execute();

  InboxMessage execute(Consumer<InboxMessage> postUpdateAction);
}

interface UpdateRetryAtSpec extends UpdateExecutor {
  UpdateExecutor retryAt(Instant retryAt);
}
