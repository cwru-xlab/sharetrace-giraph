package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * A general response to a request made to a PDA.
 */
@Value.Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonSerialize(as = PdaResponse.class)
@JsonDeserialize(as = PdaResponse.class)
public abstract class AbstractPdaResponse<T> implements Response<Record<T>> {

  @JsonProperty(value = "records", access = Access.READ_WRITE)
  public abstract Optional<List<Record<T>>> getData();

  @Override
  public abstract Optional<String> getError();

  @Override
  public abstract Optional<String> getCause();

  @Value.Check
  protected final void verifyInputArguments() {
    ResponseUtil.verifyInputArguments(this);
  }
}
