package main.java.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.io.IOException;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 *
 * @param <N> Numerical type of the score.
 */
@Log4j2
@Value(staticConstructor = "of")
public class RiskScore<N extends Number & Comparable<N>> implements ComputedValue<N>
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
    public int compareTo(@NonNull ComputedValue<N> o)
    {
        return score.compareTo(o.getValue());
    }
}
