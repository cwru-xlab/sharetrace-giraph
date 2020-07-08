package main.java.model;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

/**
 * A generic container for a user identifier.
 *
 * @param <T> Type of identification.
 */
@Log4j2
@Value(staticConstructor = "of")
public class UserId<T extends Comparable<T>> implements Identifiable<T>
{
    @NonNull
    @Getter(AccessLevel.NONE)
    T id;

    @Override
    public T getId()
    {
        return id;
    }

    @Override
    public int compareTo(Identifiable<T> o)
    {
        return id.compareTo(o.getId());
    }
}
