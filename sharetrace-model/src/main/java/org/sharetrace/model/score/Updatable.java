package org.sharetrace.model.score;

import java.time.Instant;

@FunctionalInterface
public interface Updatable {

  Instant getUpdateTime();
}
