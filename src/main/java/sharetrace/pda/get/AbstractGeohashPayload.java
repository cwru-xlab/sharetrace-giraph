package sharetrace.pda.get;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedSet;
import org.immutables.value.Value;
import sharetrace.model.location.Geohash;

@JsonSerialize(as = GeohashPayload.class)
@JsonDeserialize(as = GeohashPayload.class)
@Value.Immutable
public abstract class AbstractGeohashPayload {

  public abstract String getEndpoint();

  public abstract String getRecordId();

  @JsonProperty(value = "data", access = Access.READ_ONLY)
  public abstract SortedSet<Geohash> getGeohashes();
}
