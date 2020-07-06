package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.*;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;

@Data(staticConstructor = "of")
@Setter(AccessLevel.PRIVATE)
public class SortedRiskScores implements Writable
{
    @NonNull
    private SortedSet<TemporalUserRiskScore<Long, Double>> sortedRiskScores;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(sortedRiskScores.size());
        for (TemporalUserRiskScore<Long, Double> score : sortedRiskScores)
        {
            dataOutput.writeLong(score.getUserId().getId());
            dataOutput.writeLong(score.getUpdateTime().toEpochMilli());
            dataOutput.writeDouble(score.getRiskScore().getValue());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        int nScores = dataInput.readInt();
        SortedSet<TemporalUserRiskScore<Long, Double>> sortedScores = new TreeSet<>();
        for (int iScore = 0; iScore < nScores; iScore++)
        {
            Identifiable<Long> userId = UserId.of(dataInput.readLong());
            Instant updateTime = Instant.ofEpochMilli(dataInput.readLong());
            ComputedValue<Double> riskScore = RiskScore.of(dataInput.readDouble());
            sortedScores.add(TemporalUserRiskScore.of(userId, updateTime, riskScore));
        }
    }
}
