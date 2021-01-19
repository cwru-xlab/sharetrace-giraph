package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;
import org.sharetrace.model.pda.Payload;

/**
 * A single unit of a successful response.
 *
 * @param <T> Type of the data contained in the response.
 * @see Payload
 */
@Value.Immutable
@JsonSerialize(as = Record.class)
@JsonDeserialize(as = Record.class)
public abstract class AbstractRecord<T> {

  private static final String INVALID_ENDPOINT_MSG = "Endpoint must not be empty String or null";

  private static final String INVALID_RECORD_ID_MSG = "Record ID must not be empty String or null";

  @JsonProperty(value = "endpoint", access = Access.READ_WRITE)
  public abstract String getEndpoint();

  @JsonProperty(value = "recordId", access = Access.READ_WRITE)
  public abstract String getRecordId();

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract Payload<T> getPayload();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getEndpoint()), INVALID_ENDPOINT_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getRecordId()), INVALID_RECORD_ID_MSG);
  }
}
