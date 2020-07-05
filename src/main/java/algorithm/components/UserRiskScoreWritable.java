package main.java.algorithm.components;

import main.java.model.UserRiskScore;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Deprecated
public class UserRiskScoreWritable implements Writable
{
    private UserRiskScore userRiskScore;

    // TODO Finalize output format
    @Override public void write(DataOutput dataOutput) throws IOException
    {
    }

    // TODO Finalize input format
    @Override public void readFields(DataInput dataInput) throws IOException
    {
    }
}
