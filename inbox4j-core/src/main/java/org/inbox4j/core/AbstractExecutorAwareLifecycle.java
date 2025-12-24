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

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

abstract class AbstractExecutorAwareLifecycle<T extends ExecutorService> implements Lifecycle {
  protected final T executor;

  protected AbstractExecutorAwareLifecycle(T executor) {
    this.executor = executor;
  }

  @Override
  public void shutdown() {
    executor.shutdown();
  }

  @Override
  public void awaitTermination(Duration timeout) throws InterruptedException {
    if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      executor.shutdownNow();
    }
  }
}
