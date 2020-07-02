package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value @Builder public class Symptom
{
    @NonNull String name;
    @NonNull Double weighting;
}
