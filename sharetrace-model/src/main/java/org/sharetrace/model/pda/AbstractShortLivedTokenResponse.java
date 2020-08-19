package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ShortLivedTokenResponse.class)
@JsonDeserialize(as = ShortLivedTokenResponse.class)
public abstract class AbstractShortLivedTokenResponse {

  private static final String INVALID_TOKEN_MSG = "Token must not be an empty String or null";

  @JsonProperty(value = "token", access = Access.READ_WRITE)
  public abstract String getShortLivedToken();

  @JsonProperty(value = "associatedHats", access = Access.READ_WRITE)
  @Value.NaturalOrder
  public abstract List<String> getAccounts();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getShortLivedToken()), INVALID_TOKEN_MSG);
  }
}
