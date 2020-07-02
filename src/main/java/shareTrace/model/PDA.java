package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Set;

// TODO Demographic info
@Value @Builder public class PDA
{
    @NonNull AccessToken<?> accessToken;
    @NonNull UserID<?> userID;
    @NonNull Set<Symptom> symptoms;
    @NonNull Boolean diagnosed;
    @NonNull ContactHistory contactHistory;
}
