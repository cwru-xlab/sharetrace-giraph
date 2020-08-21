package org.sharetrace.model.location;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = TemporalLocationRecord.class)
@JsonDeserialize(as = TemporalLocationRecord.class)
public abstract class AbstractTemporalLocationRecord {

  @JsonProperty(value = "endpoint", access = Access.READ_WRITE)
  public abstract String getEndpoint();

  @JsonProperty(value = "recordId", access = Access.READ_WRITE)
  public abstract String getRecordId();

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract TemporalLocation getData();
}
