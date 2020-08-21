package org.sharetrace.model.pda.request;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Write request to a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaWriteRequest.class)
@JsonDeserialize(as = ContractedPdaWriteRequest.class)
public abstract class AbstractContractedPdaWriteRequest {

  public abstract PdaRequestUrl getPdaRequestUrl();

  public abstract ContractedPdaWriteRequestBody getWriteRequestBody();
}
