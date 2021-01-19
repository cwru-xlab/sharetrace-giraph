package org.sharetrace.model.location;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.SortedMap;
import java.util.SortedSet;
import org.immutables.value.Value;

/**
 * A pairing of a location with a time.
 * <p>
 * Note that {@link #compareTo(AbstractTemporalLocation)} first compares by time and then by
 * location. In this way, a {@link SortedSet} or {@link SortedMap} will consider two {@link
 * TemporalLocation}s that occurred at the same time to be equal from the perspective of the
 * collection. {@link #hashCode()} still considers both time and location so a {@link HashSet} or
 * {@link HashMap} will keep two {@link TemporalLocation}s that occurred at the same time, but at
 * different locations.
 */
@Value.Immutable
@JsonSerialize(as = TemporalLocation.class)
@JsonDeserialize(as = TemporalLocation.class)
public abstract class AbstractTemporalLocation implements Comparable<AbstractTemporalLocation> {

  @JsonProperty(value = "hash", access = Access.READ_WRITE)
  public abstract String getLocation();

  @JsonProperty(value = "timestamp", access = Access.READ_WRITE)
  public abstract Instant getTime();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getLocation()));
  }

  public boolean isBefore(TemporalLocation location) {
    return getTime().isBefore(location.getTime());
  }

  public boolean isAfter(TemporalLocation location) {
    return getTime().isAfter(location.getTime());
  }

  public boolean hasSameLocation(TemporalLocation location) {
    return getLocation().equals(location.getLocation());
  }

  @Override
  public int compareTo(AbstractTemporalLocation o) {
    Preconditions.checkNotNull(o);
    int compareTo = getTime().compareTo(o.getTime());
    if (0 == compareTo) {
      compareTo = getLocation().compareTo(o.getLocation());
    }
    return compareTo;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getLocation(), getTime());
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof AbstractTemporalLocation)) {
      return false;
    }
    return 0 == compareTo((AbstractTemporalLocation) o);
  }
}
