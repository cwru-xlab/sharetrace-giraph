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
import org.sharetrace.model.pda.response.util.ResponseUtil;

/**
 * A general response to a request made to a PDA.
 */
@Value.Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonSerialize(as = PdaResponse.class)
@JsonDeserialize(as = PdaResponse.class)
public abstract class AbstractPdaResponse<T> implements Response<Record<T>> {

  @Override
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

  @Override
  @Value.Derived
  public boolean isSuccess() {
    return isDataPresent() && !isErrorPresent();
  }

  @Override
  @Value.Derived
  public boolean isError() {
    return !isDataPresent() && isErrorPresent();
  }

  @Override
  @Value.Derived
  public boolean isEmpty() {
    boolean isDataEmpty = getData().isPresent() && getData().get().isEmpty();
    return isDataEmpty && !isErrorPresent();
  }

  private boolean isDataPresent() {
    return getData().isPresent() && !getData().get().isEmpty();
  }

  private boolean isErrorPresent() {
    return getError().isPresent() && getCause().isPresent();
  }

}
