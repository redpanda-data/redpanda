package io.vectorized.kafka;

import java.io.Serializable;

public class KeyStats implements Serializable {
  long updates = 0;
  long latestValue;
  long latestValueOffset;

  @Override
  public String toString() {
    return "KeyStats{"
        + "updates=" + updates + ", latestValue=" + latestValue
        + ", latestValueOffset=" + latestValueOffset + '}';
  }
}
