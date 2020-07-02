package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.io.IOException;

@Value public class RiskScore implements Comparable<RiskScore>
{
    @NonNull Double score;
    public static final String INVALID_RISK_SCORE_MESSAGE =
            "%must be between 0" + " and 1, inclusive.";

    @Builder public RiskScore(@NonNull Double riskScore) throws IOException
    {
        verifyScore(riskScore);
        this.score = riskScore;
    }

    private void verifyScore(@NonNull Double score) throws IOException
    {
        if (score <= 0 && 1 <= score)
        {
            throw new IOException(formatInvalidRiskScoreMessage(score));
        }
    }

    private String formatInvalidRiskScoreMessage(Double score)
    {
        return score.toString() + INVALID_RISK_SCORE_MESSAGE;
    }

    @Override public int compareTo(RiskScore riskScore)
    {
        return Double.compare(getScore(), riskScore.getScore());
    }
}
