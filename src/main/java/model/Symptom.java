package model;

public class Symptom
{
    private final String name;
    private final Double weighting;

    public Symptom(String name, Double weighting)
    {
        this.name = name;
        this.weighting = weighting;
    }

    public String getName()
    {
        return name;
    }

    public Double getWeighting()
    {
        return weighting;
    }
}
