package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Contact;
import main.java.model.Identifiable;
import main.java.model.TemporalOccurrence;
import main.java.model.UserId;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;

@Deprecated
@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
public class ContactWritableComparable implements WritableComparable<ContactWritableComparable>
{
    @NonNull
    private Contact<Long, Long> contact;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(contact.getFirstUser().getId());
        dataOutput.writeLong(contact.getSecondUser().getId());
        int nOccurrences = contact.getOccurrences().size();
        dataOutput.writeInt(nOccurrences);
        for (TemporalOccurrence occurrence : contact.getOccurrences())
        {
            dataOutput.writeLong(occurrence.getTime().toEpochMilli());
            dataOutput.writeLong(occurrence.getDuration().toMillis());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        Identifiable<Long> firstUser = UserId.of(dataInput.readLong());
        Identifiable<Long> secondUser = UserId.of(dataInput.readLong());
        int nOccurrences = dataInput.readInt();
        SortedSet<TemporalOccurrence> occurrences = new TreeSet<>();
        for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++)
        {
            Instant time = Instant.ofEpochMilli(dataInput.readLong());
            Duration duration = Duration.ofMillis(dataInput.readLong());
            occurrences.add(TemporalOccurrence.of(time, duration));
        }
        setContact(Contact.of(firstUser, secondUser, occurrences));
    }

    @Override
    public int compareTo(ContactWritableComparable o)
    {
        return contact.compareTo(o.getContact());
    }

    @Override
    public int hashCode()
    {
        return contact.hashCode();
    }
}
