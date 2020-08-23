package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Body of a read request to a contracted PDA.
 */
@Value.Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonSerialize(as = ContractedPdaReadRequestBody.class)
@JsonDeserialize(as = ContractedPdaReadRequestBody.class)
public abstract class AbstractContractedPdaReadRequestBody {

  @JsonUnwrapped
  public abstract ContractedPdaRequestBody getBaseRequestBody();

  @JsonUnwrapped
  public abstract Optional<PdaReadRequestParameters> getParameters();
}
