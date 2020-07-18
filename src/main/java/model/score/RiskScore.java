package model.score;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 */
@Log4j2
@Data
@Setter(AccessLevel.NONE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class RiskScore implements ComputedValue<Double>, Writable
{
    public static final String INVALID_RISK_SCORE_MESSAGE = " must be between 0 and 1, inclusive.";

    @Getter(AccessLevel.NONE)
    private double score;

    private RiskScore(double riskScore) throws IOException
    {
        verifyScore(riskScore);
        score = riskScore;
    }

    private static String formatInvalidRiskScoreMessage(@NonNull Double score)
    {
        return MessageFormat.format("{0}{1}", score, INVALID_RISK_SCORE_MESSAGE);
    }

    private static void verifyScore(double riskScore) throws IOException
    {
        if (0.0 >= riskScore && 1.0 <= riskScore)
        {
            throw new IOException(formatInvalidRiskScoreMessage(riskScore));
        }
    }

    public static RiskScore fromDataInput(@NonNull DataInput dataInput) throws IOException
    {
        RiskScore riskScore = new RiskScore();
        riskScore.readFields(dataInput);
        return riskScore;
    }

    @Override
    public void readFields(@NonNull DataInput dataInput) throws IOException
    {
        score = dataInput.readDouble();
    }

    @Override
    public void write(@NonNull DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(score);
    }

    @Override
    public int compareTo(@NonNull ComputedValue<Double> o)
    {
        return Double.compare(score, o.getValue());
    }

    @Override
    public ComputedValue<Double> minus(@NonNull ComputedValue<Double> value) throws IOException
    {
        return of(score - value.getValue());
    }

    public static RiskScore of(double riskScore) throws IOException
    {
        return new RiskScore(riskScore);
    }

    @Override
    public ComputedValue<Double> abs() throws IOException
    {
        return of(Math.abs(score));
    }

    @Override
    public Double getValue()
    {
        return score;
    }
}
