package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import java.util.List;
import java.util.Optional;

/**
 * A generic response to a request.
 * <p>
 * Depending on the type of the response, either a message or cause component may be returned. For
 * this reason, both are included as potential elements of the response.
 *
 * @param <T> Type of the data contained in the response.
 */
public interface Response<T> {

  Optional<List<T>> getData();

  @JsonAlias({"error", "message"})
  @JsonProperty(value = "error", access = Access.READ_WRITE)
  Optional<String> getError();

  @JsonProperty(value = "cause", access = Access.READ_WRITE)
  Optional<String> getCause();

  boolean isSuccess();

  boolean isError();

  boolean isEmpty();
}
