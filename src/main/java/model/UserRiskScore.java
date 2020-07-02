package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value @Builder public class UserRiskScore implements Comparable<UserRiskScore>
{
    @NonNull UserID<?> userID;
    @NonNull RiskScore riskScore;

    @Override public int compareTo(@NonNull UserRiskScore userRiskScore)
    {
        return getRiskScore().compareTo(userRiskScore.getRiskScore());
    }
}
