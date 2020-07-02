package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value @Builder public class SignalStrength
{
    @NonNull Double intensity;
}
