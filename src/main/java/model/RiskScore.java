package main.java.model;

import lombok.*;

import java.io.IOException;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 *
 * The default implementation of compareTo(RiskScore) uses the {@code double} value of the score.
 *
 * @param <N> Numerical type of the score.
 */
@Value public class RiskScore<N extends Number> implements ComputedValue<N>
{
    @NonNull @Getter(AccessLevel.NONE) N score;
    public static final String INVALID_RISK_SCORE_MESSAGE = " must be between 0 and 1, inclusive.";

    @Builder private RiskScore(@NonNull N riskScore) throws IOException
    {
        verifyScore(riskScore);
        score = riskScore;
    }

    private void verifyScore(@NonNull N riskScore) throws IOException
    {
        double risk = riskScore.doubleValue();
        if (0.0 >= risk && 1.0 <= risk)
        {
            throw new IOException(formatInvalidRiskScoreMessage(risk));
        }
    }

    private static String formatInvalidRiskScoreMessage(Double score)
    {
        return score + INVALID_RISK_SCORE_MESSAGE;
    }

    @Override public N getValue()
    {
        return score;
    }

    @Override public int compareTo(N o)
    {
        return Double.compare(score.doubleValue(), o.doubleValue());
    }
}
