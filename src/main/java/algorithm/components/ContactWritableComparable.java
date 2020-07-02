package algorithm.components;

import lombok.Data;
import lombok.NonNull;
import model.Contact;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data public class ContactWritableComparable
        implements WritableComparable<ContactWritableComparable>
{
    @NonNull private Contact contact;

    @Override public int compareTo(ContactWritableComparable o)
    {
        return getContact().compareTo(o.getContact());
    }

    @Override
    // TODO Finalize output format
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(getContact().toString());
    }

    @Override
    // TODO Finalize input format
    public void readFields(DataInput dataInput) throws IOException
    {
        dataInput.readUTF();
    }

    @Override public int hashCode()
    {
        return getContact().hashCode();
    }
}
