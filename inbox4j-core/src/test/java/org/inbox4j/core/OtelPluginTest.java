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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OtelPluginTest {

  @Test
  void encodeDecode() {
    Map<String, String> carrier = new HashMap<String, String>();
    carrier.put("key1", "value1");
    carrier.put("key2", "value2");

    String encoded = OtelPlugin.encode(carrier);
    Map<String, String> decoded = OtelPlugin.decode(encoded);

    assertThat(decoded).isEqualTo(carrier);
  }
}
