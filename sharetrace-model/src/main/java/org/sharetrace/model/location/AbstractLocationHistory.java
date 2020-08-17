package org.sharetrace.model.location;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.sharetrace.model.identity.Identifiable;

/**
 * A collection of {@link TemporalLocation}s that are associated with an id.
 */
@Value.Immutable
@JsonSerialize(as = LocationHistory.class)
@JsonDeserialize(as = LocationHistory.class)
public abstract class AbstractLocationHistory implements Identifiable<String> {

  @Override
  public abstract String getId();

  @Value.NaturalOrder
  public abstract SortedSet<TemporalLocation> getHistory();
}
