package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Identifiable;
import main.java.model.UserId;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Identifier for a {@link Vertex} that uses {@link Identifiable<Long>} and {@link UserId<Long>}.
 */
@Deprecated
@Data(staticConstructor = "of")
@Setter(AccessLevel.PRIVATE)
public class User implements WritableComparable<Identifiable<Long>>
{
    @NonNull
    private Identifiable<Long> userId;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(userId.getId());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        setUserId(UserId.of(dataInput.readLong()));
    }

    @Override
    public int compareTo(@NonNull Identifiable<Long> o)
    {
        return Long.compare(userId.getId(), o.getId());
    }
}
