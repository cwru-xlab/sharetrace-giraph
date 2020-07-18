package model.score;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;
import java.text.MessageFormat;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 */
@Log4j2
@Value
public class RiskScore implements ComputedValue<Double>
{
    public static final String INVALID_RISK_SCORE_MESSAGE = " must be between 0 and 1, inclusive.";

    @Getter(AccessLevel.NONE)
    double score;

    private RiskScore(double riskScore) throws IOException
    {
        verifyScore(riskScore);
        score = riskScore;
    }

    private static void verifyScore(double riskScore) throws IOException
    {
        if (0.0 >= riskScore && 1.0 <= riskScore)
        {
            throw new IOException(formatInvalidRiskScoreMessage(riskScore));
        }
    }

    private static String formatInvalidRiskScoreMessage(Double score)
    {
        return MessageFormat.format("{0}{1}", score, INVALID_RISK_SCORE_MESSAGE);
    }

    @Override
    public Double getValue()
    {
        return score;
    }

    @Override
    public ComputedValue<Double> minus(ComputedValue<Double> value) throws IOException
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
    public int compareTo(@NonNull ComputedValue<Double> o)
    {
        return Double.compare(score, o.getValue());
    }
}
