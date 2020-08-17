package org.sharetrace.model.score;

import java.time.Instant;
import org.immutables.value.Value;

@FunctionalInterface
public interface Updatable {

  @Value.Parameter
  Instant getUpdateTime();
}
