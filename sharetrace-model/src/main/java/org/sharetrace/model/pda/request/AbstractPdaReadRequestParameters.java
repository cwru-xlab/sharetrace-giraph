package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Optional;
import java.util.function.Predicate;
import org.immutables.value.Value;
import org.sharetrace.model.pda.PdaUtil;

/**
 * Parameters of a read request to a PDA.
 */
@Value.Immutable
@JsonInclude(Include.NON_ABSENT)
@JsonSerialize(as = PdaReadRequestParameters.class)
@JsonDeserialize(as = PdaReadRequestParameters.class)
public abstract class AbstractPdaReadRequestParameters {

  private static final String INVALID_ORDER_BY_MSG = "Order by must not be empty String or null";

  private static final int MAX_TAKE_AMOUNT = 1000;

  private static final String INVALID_TAKE_VALUE_MSG =
      "Take amount must not be greater than " + MAX_TAKE_AMOUNT;

  private static final String INVALID_SKIP_VALUE_MSG = "Skip amount must be non-negative";

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
    PdaUtil.verifyOptionalString(getOrderBy(), INVALID_ORDER_BY_MSG);
    Predicate<Integer> lessThanMax = n -> n <= MAX_TAKE_AMOUNT;
    PdaUtil.verifyOptionalNumber(getTakeAmount(), lessThanMax, INVALID_TAKE_VALUE_MSG);
    Predicate<Integer> nonNegative = n -> 0 <= n;
    PdaUtil.verifyOptionalNumber(getSkipAmount(), nonNegative, INVALID_SKIP_VALUE_MSG);
  }

  public enum Ordering {
    ASCENDING("ascending"), DESCENDING("descending");

    private final String value;

    Ordering(String value) {
      this.value = value;
    }

    @JsonValue
    public String getValue() {
      return value;
    }
  }
}
