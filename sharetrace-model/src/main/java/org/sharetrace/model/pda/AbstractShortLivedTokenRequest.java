package org.sharetrace.model.pda;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ShortLivedTokenRequest.class)
@JsonDeserialize(as = ShortLivedTokenRequest.class)
public abstract class AbstractShortLivedTokenRequest {

  private static final String INVALID_URL_MSG = "URL must not be an empty String or null";

  private static final String INVALID_TOKEN_MSG = "Token must not be empty String or null";

  public abstract String getContractsServerUrl();

  public abstract String getLongLivedToken();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getContractsServerUrl()), INVALID_URL_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getLongLivedToken()), INVALID_TOKEN_MSG);
  }
}
