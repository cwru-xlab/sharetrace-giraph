package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.sharetrace.model.pda.Payload;

/**
 * Request body of a write request to a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaWriteRequestBody.class)
@JsonDeserialize(as = ContractedPdaWriteRequestBody.class)
public abstract class AbstractContractedPdaWriteRequestBody<T> {

  @JsonUnwrapped
  public abstract ContractedPdaRequestBody getBody();

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract Payload<T> getPayload();
}
