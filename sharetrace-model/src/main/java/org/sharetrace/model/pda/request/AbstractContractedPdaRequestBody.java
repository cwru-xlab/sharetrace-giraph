package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Core elements of the request body to a contracted PDA.
 */
@Value.Immutable
@JsonSerialize(as = ContractedPdaRequestBody.class)
@JsonDeserialize(as = ContractedPdaRequestBody.class)
public interface AbstractContractedPdaRequestBody {

  @JsonProperty(value = "token", access = Access.READ_WRITE)
  String getShortLivedToken();

  @JsonProperty(value = "contractId", access = Access.READ_WRITE)
  String getContractId();

  @JsonProperty(value = "hatName", access = Access.READ_WRITE)
  String getHatName();
}
