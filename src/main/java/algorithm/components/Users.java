package main.java.algorithm.components;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NonNull;
import lombok.Setter;
import main.java.model.Identifiable;
import main.java.model.UserId;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Data(staticConstructor = "of")
@Setter(AccessLevel.PRIVATE)
public class Users implements WritableComparable<Users>
{
    @NonNull
    private Set<Identifiable<Long>> users;

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        int nUsers = users.size();
        dataOutput.writeInt(nUsers);
        for (Identifiable<Long> user : users)
        {
            dataOutput.writeLong(user.getId());
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        int nUsers = dataInput.readInt();
        Set<Identifiable<Long>> usersInput = new HashSet<>(nUsers);
        for (int iUser = 0; iUser < nUsers; iUser++)
        {
            usersInput.add(UserId.of(dataInput.readLong()));
        }
    }

    @Override
    public int compareTo(@NonNull Users o)
    {
        Iterator<Identifiable<Long>> thisIter = users.iterator();
        Iterator<Identifiable<Long>> otherIter = o.getUsers().iterator();
        int compare = 0;
        while (thisIter.hasNext() && otherIter.hasNext() && 0 == compare)
        {
            compare += thisIter.next().compareTo(otherIter.next());
        }
        return compare;
    }

}
