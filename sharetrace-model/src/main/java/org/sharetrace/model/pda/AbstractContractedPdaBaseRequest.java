package org.sharetrace.model.pda;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ContractedPdaBaseRequest.class)
@JsonDeserialize(as = ContractedPdaBaseRequest.class)
public abstract class AbstractContractedPdaBaseRequest {

  private static final String INVALID_PDA_DOMAIN = "PDA domain must not be empty String or null";

  private static final String INVALID_NAMESPACE = "Namespace must not be empty String or null";

  private static final String INVALID_ENDPOINT = "Endpoint must not be empty String or null";

  public abstract String getPdaDomain();

  public abstract String getNamespace();

  public abstract String getEndpoint();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getPdaDomain()), INVALID_PDA_DOMAIN);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getNamespace()), INVALID_NAMESPACE);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getEndpoint()), INVALID_ENDPOINT);
  }
}
