package org.inbox4j;

import java.time.Instant;
import java.time.InstantSource;

public class MutableInstantSource implements InstantSource {

  private Instant instant;

  public MutableInstantSource(Instant instant) {
    this.instant = instant;
  }

  public void setInstant(Instant instant) {
    this.instant = instant;
  }

  @Override
  public Instant instant() {
    return instant;
  }
}
