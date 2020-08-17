package org.sharetrace.model.score;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Objects;
import org.immutables.value.Value;
import org.sharetrace.model.identity.Identifiable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RiskScore} with a time for when it was last updated and an associated user.
 * <p>
 * The default implementation of {@link #compareTo(AbstractRiskScore)} is to first compare risk
 * score update time. If the risk scores are equally comparable based on the former, the value of
 * the risk scores is then used for comparison. Finally, if the risk scores are still equally
 * comparable based on the former two criteria, the {@link #getId()} ()} ()} is used for
 * comparison.
 */
@Value.Immutable
@JsonSerialize(as = RiskScore.class)
@JsonDeserialize(as = RiskScore.class)
public abstract class AbstractRiskScore implements Updatable, Identifiable<String>,
    ComputedValue<Double>, Comparable<AbstractRiskScore> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRiskScore.class);

  private static final String RISK_SCORE_RANGE_MESSAGE = "Risk score must be between 0 and 1, inclusive";

  private static final double MIN_RISK_SCORE = 0.0;

  private static final double MAX_RISK_SCORE = 1.0;

  @Override
  public abstract String getId();

  @Override
  public abstract Instant getUpdateTime();

  @Override
  public abstract Double getValue();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(MIN_RISK_SCORE <= getValue(), RISK_SCORE_RANGE_MESSAGE);
    Preconditions.checkArgument(MAX_RISK_SCORE >= getValue(), RISK_SCORE_RANGE_MESSAGE);
  }

  @Override
  public final int compareTo(AbstractRiskScore o) {
    Preconditions.checkNotNull(o);
    int compare = getUpdateTime().compareTo(o.getUpdateTime());
    if (0 == compare) {
      compare = Double.compare(getValue(), o.getValue());
      if (0 == compare) {
        compare = getId().compareTo(o.getId());
      }
    }
    return compare;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId(), getUpdateTime(), getValue());
  }

  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof AbstractRiskScore)) {
      return false;
    }
    return 0 == compareTo((AbstractRiskScore) o);
  }
}
