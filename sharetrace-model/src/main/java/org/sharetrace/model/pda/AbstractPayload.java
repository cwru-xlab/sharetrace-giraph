package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;

/**
 * Data contained in a successful response to a request.
 *
 * @param <T> Type of the data contained in the response.
 */
@Value.Immutable
@JsonSerialize(as = Payload.class)
@JsonDeserialize(as = Payload.class)
public abstract class AbstractPayload<T> {

  @JsonProperty(value = "data", access = Access.READ_WRITE)
  public abstract T getData();
}
