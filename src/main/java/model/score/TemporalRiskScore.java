package model.score;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated.
 * <p>
 * The default implementation of {@link #compareTo(TemporalRiskScore)} is to first compare {@link #riskScore}. If the
 * risk scores are equally comparable based on the former, then {@link #updateTime} is then used for comparison.
 */
@Log4j2
@Value(staticConstructor = "of")
public class TemporalRiskScore implements Comparable<TemporalRiskScore>
{
    @NonNull
    Instant updateTime;

    @NonNull
    RiskScore riskScore;

    @Override
    public int compareTo(@NonNull TemporalRiskScore o)
    {
        int compare = updateTime.compareTo(o.getUpdateTime());
        if (0 == compare)
        {
            compare = riskScore.compareTo(o.getRiskScore());
        }
        return compare;
    }
}
