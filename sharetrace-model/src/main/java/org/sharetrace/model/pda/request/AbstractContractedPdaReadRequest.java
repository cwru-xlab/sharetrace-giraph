package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Request to read data from a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaReadRequest.class)
@JsonDeserialize(as = ContractedPdaReadRequest.class)
public abstract class AbstractContractedPdaReadRequest {

  public abstract PdaRequestUrl getUrl();

  public abstract ContractedPdaReadRequestBody getReadBody();
}
