package org.inbox4j;

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