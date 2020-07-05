package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.ComputedValue;
import main.java.model.RiskScore;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A {@link RiskScore} stored in a {@link Vertex} and used as part of the risk score computation.
 */
@Deprecated
@Data
@Setter(AccessLevel.PRIVATE)
public class RiskScoreWritable implements Writable
{
    @NonNull
    private ComputedValue<Double> riskScore;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeDouble(riskScore.getValue());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        setRiskScore(RiskScore.of(dataInput.readDouble()));
    }
}
