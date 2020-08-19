package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ContractedPdaReadRequest.class)
@JsonDeserialize(as = ContractedPdaReadRequest.class)
public abstract class AbstractContractedPdaReadRequest implements ContractedPdaRequest {

  @Override
  @JsonUnwrapped
  public abstract ContractedPdaBaseRequest getBaseRequest();

  @Override
  @JsonUnwrapped
  public abstract ContractedPdaBaseRequestBody getBaseRequestBody();

  @JsonUnwrapped
  public abstract Optional<PdaGetRequestBody> getPdaGetRequestBody();
}
