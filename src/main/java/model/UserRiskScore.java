package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Deprecated
@Value
@Builder
public class UserRiskScore<U, R extends Number> implements Comparable<ComputedValue<R>>
{
    @NonNull
    Identifiable<U> userId;

    @NonNull
    ComputedValue<R> riskScore;

    @Override
    public int compareTo(ComputedValue<R> o)
    {
        return riskScore.compareTo(o.getValue());
    }
}
