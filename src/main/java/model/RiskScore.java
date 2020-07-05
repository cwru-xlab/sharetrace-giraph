package main.java.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.io.IOException;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 * <p>
 * The default implementation of compareTo(RiskScore) uses the {@code double} value of the score.
 *
 * @param <N> Numerical type of the score.
 */
@Value(staticConstructor = "of")
public class RiskScore<N extends Number> implements ComputedValue<N>
{
    @NonNull
    @Getter(AccessLevel.NONE)
    N score;

    public static final String INVALID_RISK_SCORE_MESSAGE = " must be between 0 and 1, inclusive.";

    private RiskScore(@NonNull N score) throws IOException
    {
        verifyScore(score);
        this.score = score;
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

    @Override
    public N getValue()
    {
        return score;
    }

    @Override
    public int compareTo(N o)
    {
        return Double.compare(score.doubleValue(), o.doubleValue());
    }
}
