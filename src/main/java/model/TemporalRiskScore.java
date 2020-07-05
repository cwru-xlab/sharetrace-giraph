package main.java.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated. The value returned by {@link #getValue()} is the
 * risk score, not {@link #updateTime}.
 *
 * @param <N> Numerical type of the risk score.
 */
@Value(staticConstructor = "of")
public class TemporalRiskScore<N extends Number> implements ComputedValue<N>
{
    @NonNull
    Instant updateTime;

    @NonNull
    @Getter(AccessLevel.NONE)
    ComputedValue<N> riskScore;

    @Override
    public N getValue()
    {
        return riskScore.getValue();
    }

    @Override
    public int compareTo(@NonNull N o)
    {
        return riskScore.compareTo(o);
    }
}
