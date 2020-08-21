package org.sharetrace.model.location;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedSet;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = RawLocationHistory.class)
@JsonDeserialize(as = RawLocationHistory.class)
public abstract class AbstractRawLocationHistory {

  @JsonUnwrapped
  @Value.NaturalOrder
  public abstract SortedSet<RawTemporalLocation> getRawHistory();
}
