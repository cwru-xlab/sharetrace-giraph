package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Optional;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ContractedPdaResponse.class)
@JsonDeserialize(as = ContractedPdaResponse.class)
public abstract class AbstractContractedPdaResponse {

  private static final String INVALID_ENDPOINT_MSG = "Endpoint must not be empty String or null";

  private static final String INVALID_RECORD_ID_MSG = "Record ID must not be empty String or null";

  @JsonProperty(value = "endpoint", access = Access.READ_WRITE)
  public abstract Optional<String> getEndpoint();

  @JsonProperty(value = "recordId", access = Access.READ_WRITE)
  public abstract Optional<String> getRecordId();

  @Value.Default
  @JsonAnyGetter
  public Map<String, String> getData() {
    return ImmutableMap.of();
  }

  @Value.Check
  protected final void verifyInputArguments() {
    if (getEndpoint().isPresent()) {
      String endpoint = getEndpoint().get();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(endpoint), INVALID_ENDPOINT_MSG);
    }
    if (getRecordId().isPresent()) {
      String recordId = getRecordId().get();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(recordId), INVALID_RECORD_ID_MSG);
    }
  }
}
