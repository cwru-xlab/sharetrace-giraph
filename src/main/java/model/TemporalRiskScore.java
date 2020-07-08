package main.java.model;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated.
 * <p>
 * The default implementation of {@link #compareTo(TemporalRiskScore)} is to first compare {@link #riskScore}. If the
 * risk scores are equally comparable based on the former, then {@link #updateTime} is then used for comparison.
 *
 * @param <N> Numerical type of the risk score.
 */
@Log4j2
@Value(staticConstructor = "of")
public class TemporalRiskScore<N extends Number & Comparable<N>> implements Comparable<TemporalRiskScore<N>>
{
    @NonNull
    Instant updateTime;

    @NonNull
    ComputedValue<N> riskScore;

    @Override
    public int compareTo(@NonNull TemporalRiskScore<N> o)
    {
        int compare = updateTime.compareTo(o.getUpdateTime());
        if (0 == compare)
        {
            compare = riskScore.compareTo(o.getRiskScore());
        }
        return compare;
    }
}
