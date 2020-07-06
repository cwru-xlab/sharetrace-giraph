package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Contact;
import main.java.model.Identifiable;
import main.java.model.UserId;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * A contact ID is defined by the IDs of its two users.
 * <p>
 * Two {@code ContactIdWritableComparable}s are considered equal if they contain the same two {@link Identifiable},
 * regardless of the field they are assigned.
 *
 * @see Contact
 */
@Deprecated
@Data
@Setter(AccessLevel.PRIVATE)
public class ContactUsers implements WritableComparable<ContactUsers>
{
    @NonNull
    private Identifiable<Long> firstUser;

    @NonNull
    private Identifiable<Long> secondUser;

    @Override
    public void write(@NonNull DataOutput dataOutput) throws IOException
    {
        dataOutput.writeLong(firstUser.getId());
        dataOutput.writeLong(secondUser.getId());
    }

    @Override
    public void readFields(@NonNull DataInput dataInput) throws IOException
    {
        setFirstUser(UserId.of(dataInput.readLong()));
        setSecondUser(UserId.of(dataInput.readLong()));
    }

    @Override
    public int compareTo(@NonNull ContactUsers o)
    {
        int compare = firstUser.compareTo(o.getFirstUser());
        if (0 == compare)
        {
            compare = secondUser.compareTo(o.getSecondUser());
        }
        return compare;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ContactUsers that = (ContactUsers) o;
        boolean withSameOrder = firstUser.equals(that.getFirstUser()) && secondUser.equals(that.getSecondUser());
        boolean withDiffOrder = firstUser.equals(that.getSecondUser()) && secondUser.equals(that.getFirstUser());
        return withSameOrder || withDiffOrder;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(firstUser, secondUser);
    }
}
