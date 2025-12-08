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

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A plugin for Open Telemetry integration. If the plugin detects Open Telemetry on the class path
 * then it plugs in the necessary integration classes.
 */
class OtelPlugin {

  static final String OTEL_CONTEXT_CLASS_NAME = "io.opentelemetry.context.Context";
  private static final String SEPARATOR = ": ";

  private final ContextInstaller contextInstaller;
  private final ContextInjector contextInjector;

  private static boolean isOtelEnabled() {
    try {
      Class.forName(OTEL_CONTEXT_CLASS_NAME); // check presence on the class path
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  OtelPlugin() {
    this(isOtelEnabled());
  }

  OtelPlugin(boolean otelEnabled) {
    if (otelEnabled) {
      contextInstaller = new OtelContextInstaller();
      contextInjector = new OtelContextInjector();
    } else {
      contextInstaller = new NoopContextInstaller();
      contextInjector = new NoopContextInjector();
    }
  }

  public ContextInstaller getContextInstaller() {
    return contextInstaller;
  }

  public ContextInjector getContextInjector() {
    return contextInjector;
  }

  interface ContextInstaller {

    void install(String encodedContext, Runnable action);
  }

  interface ContextInjector {

    void inject(PreparedStatement statement, int parameterIndex) throws SQLException;
  }

  private static class NoopContextInstaller implements ContextInstaller {

    @Override
    public void install(String encodedContext, Runnable action) {
      action.run();
    }
  }

  private static class OtelContextInstaller implements ContextInstaller {

    private final ContextPropagators propagators;
    private final Getter getter = new Getter();

    OtelContextInstaller() {
      this.propagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    }

    @Override
    public void install(String encodedContext, Runnable action) {
      Context context =
          propagators.getTextMapPropagator().extract(Context.current(), encodedContext, getter);

      Span span =
          GlobalOpenTelemetry.getTracer(OtelContextInstaller.class.getName())
              .spanBuilder("inbox")
              .setParent(context)
              .startSpan();

      try (Scope ignore = span.makeCurrent()) {
        action.run();
      } catch (Exception exception) {
        span.recordException(exception);
        span.setStatus(StatusCode.ERROR);
        throw exception;
      } finally {
        span.end();
      }
    }
  }

  private static class NoopContextInjector implements ContextInjector {

    @Override
    public void inject(PreparedStatement statement, int parameterIndex) throws SQLException {
      statement.setNull(parameterIndex, java.sql.Types.VARCHAR);
    }
  }

  private static class OtelContextInjector implements ContextInjector {

    private final TextMapSetter<Map<String, String>> setter = new MapSetter();
    private final ContextPropagators propagators;

    OtelContextInjector() {
      this.propagators = ContextPropagators.create(W3CTraceContextPropagator.getInstance());
    }

    @Override
    public void inject(PreparedStatement statement, int parameterIndex) throws SQLException {
      java.util.Map<String, String> contextCarrier = new HashMap<>();
      propagators.getTextMapPropagator().inject(Context.current(), contextCarrier, setter);
      statement.setString(parameterIndex, encode(contextCarrier));
    }
  }

  private static class MapSetter implements TextMapSetter<Map<String, String>> {

    @Override
    public void set(java.util.Map<String, String> carrier, String key, String value) {
      if (carrier != null) {
        carrier.put(key, value);
      }
    }
  }

  private static class Getter implements TextMapGetter<String> {

    @Override
    public Iterable<String> keys(String carrier) {
      return decode(carrier).keySet();
    }

    @Override
    public String get(String carrier, String key) {
      return decode(carrier).get(key);
    }
  }

  static String encode(Map<String, String> carrier) {
    List<String> lines = new ArrayList<>(carrier.size());
    for (Map.Entry<String, String> entry : carrier.entrySet()) {
      lines.add(entry.getKey() + SEPARATOR + entry.getValue());
    }
    return String.join("\n", lines);
  }

  static Map<String, String> decode(String serialized) {
    if (serialized == null) {
      return Map.of();
    }

    Map<String, String> carrier = new HashMap<>();
    for (String line : serialized.split("\n")) {
      int index = line.indexOf(SEPARATOR);
      if (index >= 0) {
        String key = line.substring(0, index).trim();
        String value = line.substring(index + SEPARATOR.length());
        carrier.put(key, value);
      }
    }
    return carrier;
  }
}
