package main.java.algorithm.components;

import lombok.NonNull;
import lombok.Value;
import main.java.model.Contact;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Value public class ContactWritable implements Writable
{
    @NonNull Contact contact;

    // TOOD Finalize output format
    @Override public void write(DataOutput dataOutput) throws IOException
    {
    }

    // TODO Finalize input format
    @Override public void readFields(DataInput dataInput) throws IOException
    {
    }
}
