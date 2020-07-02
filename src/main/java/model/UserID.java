package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value @Builder public class UserID<T>
{
    @NonNull T id;
}
