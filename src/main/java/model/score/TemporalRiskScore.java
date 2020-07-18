package model.score;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

/**
 * A {@link RiskScore} with a time for when it was last updated.
 * <p>
 * The default implementation of {@link #compareTo(TemporalRiskScore)} is to first compare {@link #riskScore}. If the
 * risk scores are equally comparable based on the former, then {@link #updateTime} is then used for comparison.
 *
 * @see RiskScore
 */
@Log4j2
@Data
@Setter(AccessLevel.NONE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TemporalRiskScore implements WritableComparable<TemporalRiskScore>
{
    @NonNull
    private Instant updateTime;

    @NonNull
    private RiskScore riskScore;

    private TemporalRiskScore(@NonNull Instant updateTime, @NonNull RiskScore riskScore)
    {
        this.updateTime = updateTime;
        this.riskScore = riskScore;
    }

    public static TemporalRiskScore of(@NonNull Instant updateTime, @NonNull RiskScore riskScore)
    {
        return new TemporalRiskScore(updateTime, riskScore);
    }

    public static TemporalRiskScore fromDataInput(@NonNull DataInput dataInput) throws IOException
    {
        TemporalRiskScore riskScore = new TemporalRiskScore();
        riskScore.readFields(dataInput);
        return riskScore;
    }

    @Override
    public void readFields(@NonNull DataInput dataInput) throws IOException
    {
        updateTime = Instant.ofEpochMilli(dataInput.readLong());
        riskScore = RiskScore.fromDataInput(dataInput);
    }

    @Override
    public void write(@NonNull DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(updateTime.toEpochMilli());
        riskScore.write(dataOutput);
    }

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
