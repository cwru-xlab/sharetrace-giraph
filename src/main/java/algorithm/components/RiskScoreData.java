package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import main.java.model.*;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;

/**
 * A {@link TemporalUserRiskScore} stored in a {@link Vertex} and used as part of the risk score computation.
 */
@Log4j2
@Data(staticConstructor = "of")
@Setter(AccessLevel.PRIVATE)
public final class RiskScoreData implements WritableComparable<RiskScoreData>
{
    @NonNull
    TemporalUserRiskScore<Long, Double> riskScore;

    @Override
    public void write(@NonNull DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(riskScore.getUserId().getId());
        dataOutput.writeLong(riskScore.getUpdateTime().toEpochMilli());
        dataOutput.writeDouble(riskScore.getRiskScore().getValue());
    }

    @Override
    public void readFields(@NonNull DataInput dataInput) throws IOException
    {
        Identifiable<Long> user = UserId.of(dataInput.readLong());
        Instant updateTime = Instant.ofEpochMilli(dataInput.readLong());
        ComputedValue<Double> score = RiskScore.of(dataInput.readDouble());
        setRiskScore(TemporalUserRiskScore.of(user, updateTime, score));
    }

    @Override
    public int compareTo(@NonNull RiskScoreData o)
    {
        return riskScore.compareTo(o.getRiskScore());
    }
}
