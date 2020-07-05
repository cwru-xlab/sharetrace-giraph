package main.java.algorithm.components;

import lombok.Data;
import lombok.NonNull;
import main.java.model.UserRiskScore;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
@Data
public class UserRiskScoreWritableComparable implements WritableComparable<UserRiskScoreWritableComparable>
{
    @NonNull
    private UserRiskScore<Long, Double> userRiskScore;

    @Override
    public int compareTo(UserRiskScoreWritableComparable o)
    {
        return userRiskScore.compareTo(o.getUserRiskScore().getRiskScore());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
    }
}
