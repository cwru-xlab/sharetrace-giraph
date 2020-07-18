package model.score;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import model.identity.UserId;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
@Data
@Setter(AccessLevel.NONE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TemporalUserRiskScore implements WritableComparable<TemporalUserRiskScore>
{
    private UserId userId;

    private TemporalRiskScore temporalRiskScore;

    private TemporalUserRiskScore(UserId userId, TemporalRiskScore temporalRiskScore)
    {
        this.userId = userId;
        this.temporalRiskScore = temporalRiskScore;
    }

    public static TemporalUserRiskScore of(@NonNull UserId userId, @NonNull Instant updateTime,
                                           @NonNull RiskScore riskScore)
    {
        return new TemporalUserRiskScore(UserId.of(userId.getId()), TemporalRiskScore.of(updateTime, riskScore));
    }

    public static TemporalUserRiskScore fromDataInput(DataInput dataInput) throws IOException
    {
        TemporalUserRiskScore riskScore = new TemporalUserRiskScore();
        riskScore.readFields(dataInput);
        return riskScore;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        userId = UserId.fromDataInput(dataInput);
        temporalRiskScore = TemporalRiskScore.fromDataInput(dataInput);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        userId.write(dataOutput);
        temporalRiskScore.write(dataOutput);
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
