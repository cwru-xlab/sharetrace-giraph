package model.identity;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * An identifier for a user.
 */
@Log4j2
@Data
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(staticName = "of", onConstructor = @__(@NonNull))
public class UserId implements Identifiable<String>, Writable
{
    @NonNull
    @Getter(AccessLevel.NONE)
    private String id;

    public static UserId fromDataInput(DataInput dataInput) throws IOException
    {
        UserId user = new UserId();
        user.readFields(dataInput);
        return user;
    }

    @Override
    public final void readFields(DataInput dataInput) throws IOException
    {
        id = dataInput.readUTF();
    }

    @Override
    public final String getId()
    {
        return id;
    }

    @Override
    public final int compareTo(@NonNull Identifiable<String> o)
    {
        return id.compareTo(o.getId());
    }

    @Override
    public final void write(DataOutput dataOutput) throws IOException
    {
        dataOutput.writeUTF(id);
    }
}
