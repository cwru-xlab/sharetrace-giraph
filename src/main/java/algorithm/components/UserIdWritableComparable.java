package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Identifiable;
import main.java.model.UserId;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Identifier for a vertex that uses {@link Identifiable<Long>} and {@link UserId<Long>}.
 */
@Getter(AccessLevel.PUBLIC) @Setter(AccessLevel.PRIVATE) public class UserIdWritableComparable
        implements WritableComparable<Identifiable<Long>>
{
    @NonNull private Identifiable<Long> userId;

    @Override public void write(DataOutput dataOutput) throws IOException
    {
        long id = userId.getId();
        dataOutput.writeLong(id);
    }

    @Override public void readFields(DataInput dataInput) throws IOException
    {
        long rawId = dataInput.readLong();
        Identifiable<Long> id = UserId.of(rawId);
        setUserId(id);
    }

    @Override public int compareTo(@NonNull Identifiable<Long> o)
    {
        return Long.compare(userId.getId(), o.getId());
    }
}
