package org.sharetrace.model.pda.request;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Write request to a contracted PDA.
 * @param <T> Type of the data contained in the response.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaWriteRequest.class)
@JsonDeserialize(as = ContractedPdaWriteRequest.class)
public abstract class AbstractContractedPdaWriteRequest<T> {

  public abstract PdaRequestUrl getUrl();

  public abstract ContractedPdaWriteRequestBody<T> getWriteBody();
}
