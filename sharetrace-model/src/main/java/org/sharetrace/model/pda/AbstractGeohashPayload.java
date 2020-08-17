package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.sharetrace.model.location.TemporalLocation;

@Value.Immutable
@JsonSerialize(as = GeohashPayload.class)
@JsonDeserialize(as = GeohashPayload.class)
public abstract class AbstractGeohashPayload {

  public abstract String getEndpoint();

  public abstract String getRecordId();

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract SortedSet<TemporalLocation> getLocations();
}
