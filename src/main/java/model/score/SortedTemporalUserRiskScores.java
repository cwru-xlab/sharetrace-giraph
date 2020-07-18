package model.score;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;

@Log4j2
@Data
@Setter(AccessLevel.NONE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SortedTemporalUserRiskScores implements Writable
{
    private SortedSet<TemporalUserRiskScore> riskScores;

    private SortedTemporalUserRiskScores(@NonNull SortedSet<TemporalUserRiskScore> riskScores)
    {
        this.riskScores = Collections.unmodifiableSortedSet(riskScores);
    }

    public static SortedTemporalUserRiskScores of(@NonNull SortedSet<TemporalUserRiskScore> riskScores)
    {
        return new SortedTemporalUserRiskScores(riskScores);
    }

    public static SortedTemporalUserRiskScores fromDataInput(@NonNull DataInput dataInput) throws IOException
    {
        SortedTemporalUserRiskScores riskScores = new SortedTemporalUserRiskScores();
        riskScores.readFields(dataInput);
        return riskScores;
    }

    @Override
    public void readFields(@NonNull DataInput dataInput) throws IOException
    {
        SortedSet<TemporalUserRiskScore> scores = new TreeSet<>();
        int nScores = dataInput.readInt();
        for (int iScore = 0; iScore < nScores; iScore++)
        {
            scores.add(TemporalUserRiskScore.fromDataInput(dataInput));
        }
        riskScores = Collections.unmodifiableSortedSet(scores);
    }

    @Override
    public void write(@NonNull DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(riskScores.size());
        for (TemporalUserRiskScore riskScore : riskScores)
        {
            riskScore.write(dataOutput);
        }
    }
}
