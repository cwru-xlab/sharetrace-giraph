package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

public interface ContractedPdaRequest {

  @JsonUnwrapped
  ContractedPdaBaseRequest getBaseRequest();

  @JsonUnwrapped
  ContractedPdaBaseRequestBody getBaseRequestBody();
}
