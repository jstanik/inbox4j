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

import java.util.Objects;

/**
 * A reference to a continuation.
 *
 * <p>This reference uniquely identifies a single continuation. It must be included when reporting
 * the completion of the continuation.
 *
 * <p>Use {@link #toString()} to serialize this reference for storage or transmission.
 */
public final class ContinuationReference {
  private final String value;

  /**
   * Creates a new {@code ContinuationReference} from its serialized representation.
   *
   * <p>The provided value must be a string previously obtained from {@link #toString()}. The
   * returned instance represents the same continuation as the original reference.
   *
   * @param value the serialized representation of a continuation reference
   * @return a {@link ContinuationReference} corresponding to the given value
   * @see #toString()
   */
  public static ContinuationReference fromString(String value) {
    return new ContinuationReference(value);
  }

  private ContinuationReference(String value) {
    this.value = value;
  }

  /**
   * Returns a stable serialized representation of this reference.
   *
   * <p>The returned value can later be used to recreate an equivalent instance via {@link
   * #fromString(String)}.
   *
   * @return the serialized representation of this reference
   * @see #fromString(String)
   */
  @Override
  public String toString() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ContinuationReference that)) return false;
    return Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(value);
  }
}
