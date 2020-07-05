package main.java.model;

import lombok.NonNull;
import lombok.Value;

import java.util.Set;

/**
 * A grouping of {@link Contact}s that intended is intended to be for a the same user; however, no such restriction
 * is applied at the time of instantiation.
 *
 * @param <U1> Type of identifier for the first user of {@link Contact}.
 * @param <U2> Type of identifier for the second user of a {@link Contact}.
 */
@Value(staticConstructor = "of")
public class ContactHistory<U1, U2>
{
    @NonNull
    Set<Contact<U1, U2>> history;
}
