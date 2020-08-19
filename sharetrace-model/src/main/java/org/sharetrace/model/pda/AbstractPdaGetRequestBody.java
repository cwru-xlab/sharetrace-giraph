package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = PdaGetRequestBody.class)
@JsonDeserialize(as = PdaGetRequestBody.class)
public abstract class AbstractPdaGetRequestBody {

  @JsonProperty(value = "orderBy", access = Access.READ_WRITE)
  public abstract Optional<String> getOrderBy();

  @JsonProperty(value = "ordering", access = Access.READ_WRITE)
  public abstract Optional<Ordering> getOrdering();

  @JsonProperty(value = "take", access = Access.READ_WRITE)
  public abstract Optional<Integer> getTakeAmount();

  @JsonProperty(value = "skip", access = Access.READ_WRITE)
  public abstract Optional<Long> getSkipAmount();

  public enum Ordering {
    ASCENDING, DESCENDING;
  }

}
