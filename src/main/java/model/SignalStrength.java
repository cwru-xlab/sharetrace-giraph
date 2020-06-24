package model;

public class SignalStrength
{
    private final Double intensity;

    private SignalStrength(Double intensity)
    {
        this.intensity = intensity;
    }

    public SignalStrength create(Double intensity)
    {
        return new SignalStrength(intensity);
    }

    public SignalStrength copy()
    {
        return create(getIntensity());
    }

    public Double getIntensity()
    {
        return intensity;
    }
}
