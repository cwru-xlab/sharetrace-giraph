package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Contact;
import main.java.model.TemporalOccurrence;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * {@link Contact} data stored in a {@link Vertex} and used as part of the risk score computation.
 *
 * @see Contact
 */
@Data
@Setter(AccessLevel.PRIVATE)
public class ContactData implements Writable
{
    @NonNull
    private SortedSet<TemporalOccurrence> occurrences;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(occurrences.size());
        for (TemporalOccurrence occurrence : occurrences)
        {
            dataOutput.writeLong(occurrence.getTime().toEpochMilli());
            dataOutput.writeLong(occurrence.getDuration().toMillis());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        int nOccurrences = dataInput.readInt();
        SortedSet<TemporalOccurrence> temporalOccurrences = new TreeSet<>();
        for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++)
        {
            Instant time = Instant.ofEpochMilli(dataInput.readLong());
            Duration duration = Duration.ofMillis(dataInput.readLong());
            temporalOccurrences.add(TemporalOccurrence.of(time, duration));
        }
        setOccurrences(temporalOccurrences);
    }
}
