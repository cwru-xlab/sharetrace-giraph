package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder
public class PDA<T extends Comparable<T>, U extends Comparable<U>, R extends Number>
{
    @NonNull
    Identifiable<T> accessToken;

    @NonNull
    Identifiable<U> userId;

    @NonNull
    ComputedValue<R> riskScore;

    @NonNull
    boolean diagnosed;
}