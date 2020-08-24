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

  @Value.Check
  protected final void verifyInputArguments() {
    ResponseUtil.verifyInputArguments(this);
  }
}
