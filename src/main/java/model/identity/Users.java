package model.identity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

@Log4j2
@Data
@AllArgsConstructor(access = AccessLevel.PRIVATE, onConstructor = @__(@NonNull))
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@Setter(AccessLevel.NONE)
public final class Users implements WritableComparable<Users>
{
    private SortedSet<UserId> users;

    public static Users fromDataInput(DataInput dataInput) throws IOException
    {
        Users allUsers = new Users();
        allUsers.readFields(dataInput);
        return allUsers;
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException
    {
        int nUsers = dataInput.readInt();
        SortedSet<UserId> usersInput = new TreeSet<>();
        for (int iUser = 0; iUser < nUsers; iUser++)
        {
            usersInput.add(UserId.fromDataInput(dataInput));
        }
        users = usersInput;
    }

    public static Users of(@NonNull SortedSet<UserId> users)
    {
        return new Users(Collections.unmodifiableSortedSet(users));
    }

    public int getNumUsers()
    {
        return users.size();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeInt(users.size());
        for (UserId user : users)
        {
            user.write(dataOutput);
        }
    }

    @Override
    public int compareTo(@NonNull Users o)
    {
        Iterator<UserId> thisIter = users.iterator();
        Iterator<UserId> otherIter = o.getUsers().iterator();
        int compare = 0;
        while (thisIter.hasNext() && otherIter.hasNext() && 0 == compare)
        {
            compare += thisIter.next().compareTo(otherIter.next());
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
        Users otherUsers = (Users) o;
        return users.equals(otherUsers.getUsers());
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(users);
    }
}
