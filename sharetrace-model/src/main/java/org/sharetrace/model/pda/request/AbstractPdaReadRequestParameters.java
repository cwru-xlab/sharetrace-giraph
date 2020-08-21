package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Optional;
import org.immutables.value.Value;

/**
 * Parameters of a read request to a PDA.
 */
@Value.Immutable
@JsonSerialize(as = PdaReadRequestParameters.class)
@JsonDeserialize(as = PdaReadRequestParameters.class)
public abstract class AbstractPdaReadRequestParameters {

  private static final String INVALID_ORDER_BY_MSG = "Order by must not be empty String or null";

  private static final int MAX_TAKE_AMOUNT = 1000;

  private static final String INVALID_TAKE_MSG =
      "Take amount must not be greater than " + MAX_TAKE_AMOUNT;

  private static final String INVALID_SKIP_MSG = "Skip amount must be non-negative";

  @JsonProperty(value = "orderBy", access = Access.READ_WRITE)
  public abstract Optional<String> getOrderBy();

  @JsonProperty(value = "ordering", access = Access.READ_WRITE)
  public abstract Optional<Ordering> getOrdering();

  @JsonProperty(value = "take", access = Access.READ_WRITE)
  public abstract Optional<Integer> getTakeAmount();

  @JsonProperty(value = "skip", access = Access.READ_WRITE)
  public abstract Optional<Integer> getSkipAmount();

  @Value.Check
  protected final void verifyInputArguments() {
    if (getOrderBy().isPresent()) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(getOrderBy().get()), INVALID_ORDER_BY_MSG);
    }
    if (getTakeAmount().isPresent()) {
      Preconditions.checkArgument(getTakeAmount().get() <= MAX_TAKE_AMOUNT, INVALID_TAKE_MSG);
    }
    if (getSkipAmount().isPresent()) {
      Preconditions.checkArgument(0 <= getSkipAmount().get(), INVALID_SKIP_MSG);
    }
  }

  public enum Ordering {
    ASCENDING, DESCENDING;
  }

}
