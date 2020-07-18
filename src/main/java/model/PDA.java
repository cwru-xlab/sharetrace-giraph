package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import model.identity.AccessToken;
import model.identity.UserId;
import model.score.RiskScore;

@Value
@Builder
public class PDA
{
    @NonNull
    AccessToken accessToken;

    @NonNull
    UserId userId;

    @NonNull
    RiskScore riskScore;

    boolean diagnosed;
}