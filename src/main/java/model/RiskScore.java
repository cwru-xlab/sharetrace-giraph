package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.io.IOException;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 *
 * The default implementation of compareTo(RiskScore<N>) uses the {@code double} value of the score.
 *
 * @param <N> Numerical type of the score.
 */
@Value @Builder public class RiskScore<N extends Number> implements Comparable<RiskScore<N>>
{
    @NonNull N score;
    public static final String INVALID_RISK_SCORE_MESSAGE = "must be between 0" + " and 1, inclusive.";

    @Builder public RiskScore(@NonNull N riskScore) throws IOException
    {
        verifyScore(riskScore);
        this.score = riskScore;
    }

    private void verifyScore(@NonNull N score) throws IOException
    {
        double risk = score.doubleValue();
        if (risk <= 0 && 1 <= risk)
        {
            throw new IOException(formatInvalidRiskScoreMessage(risk));
        }
    }

    private String formatInvalidRiskScoreMessage(Double score)
    {
        return score.toString() + INVALID_RISK_SCORE_MESSAGE;
    }

    @Override public int compareTo(RiskScore<N> riskScore)
    {
        return Double.compare(getScore().doubleValue(), riskScore.getScore().doubleValue());
    }
}
