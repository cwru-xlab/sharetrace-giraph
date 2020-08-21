package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Body of a read request to a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaReadRequestBody.class)
@JsonDeserialize(as = ContractedPdaReadRequestBody.class)
public abstract class AbstractContractedPdaReadRequestBody implements
    AbstractContractedPdaRequestBody {

  private static final String INVALID_TOKEN_MSG = "Token must not be empty String or null";

  private static final String INVALID_CONTRACT_ID = "Contract ID must not be empty String or null";

  private static final String INVALID_HAT_NAME = "HAT name must not be empty String or null";

  @Override
  @JsonProperty(value = "token", access = Access.READ_WRITE)
  public abstract String getShortLivedToken();

  @Override
  @JsonProperty(value = "contractId", access = Access.READ_WRITE)
  public abstract String getContractId();

  @Override
  @JsonProperty(value = "hatName", access = Access.READ_WRITE)
  public abstract String getHatName();

  @JsonUnwrapped
  public abstract Optional<PdaReadRequestParameters> getParameters();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getShortLivedToken()), INVALID_TOKEN_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getContractId()), INVALID_CONTRACT_ID);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getHatName()), INVALID_HAT_NAME);
  }

}
