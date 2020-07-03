package main.java.model;

import lombok.Data;
import lombok.NonNull;

import java.io.*;
import java.io.ObjectInputStream.GetField;

/**
 * A generic container for a user identifier.
 * @param <T> Type of identification.
 */
@Data(staticConstructor = "of") public final class UserId<T> implements Serializable
{
    private static final long serialVersionUID = -1771231055921257556L;
    private static final String DEFAULT_ID = "DEFAULT_ID";
    private static final String INVALID_OBJECT_EXCEPTION_MESSAGE = "Stream data required for deserialization";
    @NonNull private T id;

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        out.writeFields();
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        GetField field = in.readFields();
        T id = (T) field.get("id", DEFAULT_ID);
        setId(id);
    }

    private void readObjectNoData() throws ObjectStreamException
    {
        throw new InvalidObjectException(INVALID_OBJECT_EXCEPTION_MESSAGE);
    }
}
