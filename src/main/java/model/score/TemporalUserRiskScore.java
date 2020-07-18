package model.score;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;
import model.identity.UserId;

import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated and an associated user.
 * <p>
 * The default implementation of {@link #compareTo(TemporalUserRiskScore)} is to first compare risk score update time.
 * If the risk scores are equally comparable based on the former, the value of the risk scores is then used for
 * comparison. Finally, if the risk scores are still equally comparable based on the former two criteria, the
 * {@link #userId} is used for comparison.
 *
 * @see TemporalRiskScore
 */
@Log4j2
@Value
public class TemporalUserRiskScore implements Comparable<TemporalUserRiskScore>
{
    @NonNull
    UserId userId;

    @NonNull
    TemporalRiskScore temporalRiskScore;

    private TemporalUserRiskScore(UserId user, TemporalRiskScore riskScore)
    {
        userId = user;
        temporalRiskScore = riskScore;
    }

    public static TemporalUserRiskScore of(UserId userId, Instant updateTime, RiskScore riskScore)
    {
        return new TemporalUserRiskScore(userId, TemporalRiskScore.of(updateTime, riskScore));
    }

    @Override
    public int compareTo(@NonNull TemporalUserRiskScore o)
    {
        int compare = temporalRiskScore.compareTo(o.getTemporalRiskScore());
        if (0 == compare)
        {
            compare = userId.compareTo(o.getUserId());
        }
        return compare;
    }
}
