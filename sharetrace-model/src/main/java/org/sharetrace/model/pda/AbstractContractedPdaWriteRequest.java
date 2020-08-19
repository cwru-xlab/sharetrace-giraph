package org.sharetrace.model.pda;


import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableBiMap;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ContractedPdaWriteRequest.class)
@JsonDeserialize(as = ContractedPdaWriteRequest.class)
public abstract class AbstractContractedPdaWriteRequest implements ContractedPdaRequest {

  @Override
  @JsonUnwrapped
  public abstract ContractedPdaBaseRequest getBaseRequest();

  @Override
  @JsonUnwrapped
  public abstract ContractedPdaBaseRequestBody getBaseRequestBody();

  @Value.Default
  @JsonAnyGetter
  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public Map<String, String> getData() {
    return ImmutableBiMap.of();
  }
}
