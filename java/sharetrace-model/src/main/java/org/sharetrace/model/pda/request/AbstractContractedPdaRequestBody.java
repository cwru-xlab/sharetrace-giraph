package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;

/**
 * Core elements of the request body to a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaRequestBody.class)
@JsonDeserialize(as = ContractedPdaRequestBody.class)
public abstract class AbstractContractedPdaRequestBody {

  private static final String INVALID_TOKEN_MSG = "Token must not be empty String or null";

  private static final String INVALID_CONTRACT_ID = "Contract ID must not be empty String or null";

  private static final String INVALID_HAT_NAME = "HAT name must not be empty String or null";

  @JsonProperty(value = "token", access = Access.READ_WRITE)
  public abstract String getShortLivedToken();

  @JsonProperty(value = "contractId", access = Access.READ_WRITE)
  public abstract String getContractId();

  @JsonProperty(value = "hatName", access = Access.READ_WRITE)
  public abstract String getHatName();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getShortLivedToken()), INVALID_TOKEN_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getContractId()), INVALID_CONTRACT_ID);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getHatName()), INVALID_HAT_NAME);
  }
}
