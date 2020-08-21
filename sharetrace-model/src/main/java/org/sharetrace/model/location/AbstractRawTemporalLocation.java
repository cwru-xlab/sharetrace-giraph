package org.sharetrace.model.location;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = RawTemporalLocation.class)
@JsonDeserialize(as = RawTemporalLocation.class)
public abstract class AbstractRawTemporalLocation implements
    Comparable<AbstractRawTemporalLocation> {

  @JsonProperty(value = "endpoint", access = Access.READ_WRITE)
  public abstract String getEndpoint();

  @JsonProperty(value = "recordId", access = Access.READ_WRITE)
  public abstract String getRecordId();

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract TemporalLocation getData();

  @Override
  public int compareTo(AbstractRawTemporalLocation o) {
    Preconditions.checkNotNull(o);
    return getData().compareTo(o.getData());
  }

  @Override
  public int hashCode() {
    return getData().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return getData().equals(obj);
  }
}
