package algorithm.components;

import lombok.Data;
import lombok.NonNull;
import model.UserRiskScore;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data public class UserRiskScoreWritableComparable
        implements WritableComparable<UserRiskScoreWritableComparable>
{
    @NonNull private UserRiskScore userRiskScore;

    @Override public int compareTo(UserRiskScoreWritableComparable o)
    {
        return getUserRiskScore().compareTo(o.getUserRiskScore());
    }

    @Override public void write(DataOutput dataOutput) throws IOException
    {
    }

    @Override public void readFields(DataInput dataInput) throws IOException
    {
    }
}
