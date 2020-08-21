package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Response to a short-lived token request.
 */
@Value.Immutable
@JsonSerialize(as = ShortLivedTokenResponse.class)
@JsonDeserialize(as = ShortLivedTokenResponse.class)
public abstract class AbstractShortLivedTokenResponse {

  private static final String INVALID_TOKEN_MSG = "Token must not be an empty String or null";

  @JsonProperty(value = "token", access = Access.READ_WRITE)
  public abstract Optional<String> getShortLivedToken();

  @Value.NaturalOrder
  @JsonProperty(value = "associatedHats", access = Access.READ_WRITE)
  public abstract Optional<List<String>> getHats();

  @JsonProperty(value = "error", access = Access.READ_WRITE)
  public abstract Optional<String> getError();

  @JsonProperty(value = "message", access = Access.READ_WRITE)
  public abstract Optional<String> getMessage();

  public List<Object> getEmpty() {
    return ImmutableList.of();
  }

  @Value.Check
  protected final void verifyInputArguments() {
    Optional<String> shortLivedTokenOptional = getShortLivedToken();
    if (shortLivedTokenOptional.isPresent()) {
      String shortLivedToken = shortLivedTokenOptional.get();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(shortLivedToken), INVALID_TOKEN_MSG);
    }
  }
}
