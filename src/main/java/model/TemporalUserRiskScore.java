package main.java.model;

import lombok.NonNull;
import lombok.Value;

import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated and an associated user.
 * <p>
 * The default implementation of {@link #compareTo(TemporalUserRiskScore)} is to first compare {@link #updateTime}. If
 * the risk scores are equally comparable based on the former, {@link #riskScore} is then used for comparison. Finally,
 * if the risk scores are still equally comparable based on the former two criteria, the {@link #userId} is used for
 * comparison.
 *
 * @param <N> Numerical type of the risk score.
 */
@Value(staticConstructor = "of")
public class TemporalUserRiskScore<U, N extends Number> implements Comparable<TemporalUserRiskScore<U, N>>
{
    @NonNull
    Identifiable<U> userId;

    @NonNull
    Instant updateTime;

    @NonNull
    ComputedValue<N> riskScore;

    @Override
    public int compareTo(@NonNull TemporalUserRiskScore<U, N> o)
    {
        int compare = updateTime.compareTo(o.getUpdateTime());
        if (0 == compare)
        {
            compare = riskScore.compareTo(o.getRiskScore());
            if (0 == compare)
            {
                compare = userId.compareTo(o.getUserId());
            }
        }
        return compare;
    }
}
