package sharetrace.model.location;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.time.Instant;
import java.util.Objects;
import org.immutables.value.Value;

/**
 * A pairing of a location with a time.
 */
@Value.Immutable
@JsonSerialize(as = TemporalLocation.class)
@JsonDeserialize(as = TemporalLocation.class)
public abstract class AbstractTemporalLocation implements Comparable<AbstractTemporalLocation> {

  public abstract String getLocation();

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
