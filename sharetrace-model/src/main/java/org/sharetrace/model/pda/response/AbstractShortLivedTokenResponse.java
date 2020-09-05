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
 * Response to a short-lived token request.
 */
@Value.Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonSerialize(as = ShortLivedTokenResponse.class)
@JsonDeserialize(as = ShortLivedTokenResponse.class)
public abstract class AbstractShortLivedTokenResponse implements Response<String> {

  @JsonProperty(value = "token", access = Access.READ_WRITE)
  public abstract Optional<String> getShortLivedToken();

  @Override
  @JsonProperty(value = "associatedHats", access = Access.READ_WRITE)
  public abstract Optional<List<String>> getData();

  @Override
  public abstract Optional<String> getError();

  @Override
  public abstract Optional<String> getCause();

  @Override
  @Value.Derived
  public boolean isSuccess() {
    return getShortLivedToken().isPresent() && isDataPresent() && !isErrorPresent();
  }

  @Override
  @Value.Derived
  public boolean isError() {
    return !getShortLivedToken().isPresent() && !isDataPresent() && isErrorPresent();
  }

  @Override
  @Value.Derived
  public boolean isEmpty() {
    boolean isDataEmpty = getData().isPresent() && getData().get().isEmpty();
    return isDataEmpty && !getShortLivedToken().isPresent() && !isErrorPresent();
  }

  private boolean isDataPresent() {
    return getData().isPresent() && !getData().get().isEmpty();
  }

  private boolean isErrorPresent() {
    return getError().isPresent() && getCause().isPresent();
  }

  @Value.Check
  protected final void verifyInputArguments() {
    ResponseUtil.verifyInputArguments(this);
  }
}
