package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.ComputedValue;
import main.java.model.RiskScore;
import main.java.model.TemporalRiskScore;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

/**
 * A {@link TemporalRiskScore} stored in a {@link Vertex} and used as part of the risk score computation.
 */
@Data
@Setter(AccessLevel.PRIVATE)
public class TemporalRiskScoreWritable implements Writable
{
    @NonNull
    TemporalRiskScore<Double> temporalRiskScore;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        long updateTime = temporalRiskScore.getUpdateTime().toEpochMilli();
        double riskScore = temporalRiskScore.getValue();
        dataOutput.writeLong(updateTime);
        dataOutput.writeDouble(riskScore);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        Instant updateTime = Instant.ofEpochMilli(dataInput.readLong());
        ComputedValue<Double> riskScore = RiskScore.of(dataInput.readDouble());
        setTemporalRiskScore(TemporalRiskScore.of(updateTime, riskScore));
    }
}
