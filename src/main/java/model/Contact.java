package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Collection;
import java.util.Objects;
import java.util.SortedSet;

/**
 * An encounter between two users at one or more points in time.
 *
 * Two contacts are considered equal if their contacts are equal, regardless of the field they are assigned. That is,
 * a {@code Contact} with {@link #firstUser} {@code u1} and {@link #secondUser} {@code u2} is considered equal to a
 * different {@code Contact} with equal {@code u1} and {@code u2} except assigned to the opposite field.
 *
 * @param <U1> Type of identifier of the first user.
 * @param <U2> Type of identifier of the second user.
 */
@Value public class Contact<U1, U2> implements Comparable<Contact<?, ?>>
{
    @NonNull Identifiable<U1> firstUser;
    @NonNull Identifiable<U2> secondUser;
    @NonNull SortedSet<TemporalOccurrence> occurrences;
    private static final String NO_OCCURRENCES_MESSAGE = "A contact must contain at least one occurrence.";
    private static final String IDENTICAL_USERS_MESSAGE = "A contact must contain two distinct users.";

    @Builder
    private Contact(@NonNull Identifiable<U1> firstUser,
                    @NonNull Identifiable<U2> secondUser,
                    @NonNull SortedSet<TemporalOccurrence> occurrences)
    {
        verifyUsers(firstUser, secondUser);
        verifyOccurrences(occurrences);
        this.firstUser = firstUser;
        this.secondUser = secondUser;
        this.occurrences = occurrences;
    }

    private void verifyUsers(Identifiable<U1> user1, Identifiable<U2> user2)
    {
        if (user1.equals(user2))
        {
            throw new IllegalArgumentException(IDENTICAL_USERS_MESSAGE);
        }
    }

    private static void verifyOccurrences(Collection<TemporalOccurrence> timesInContact)
    {
        if (timesInContact.isEmpty())
        {
            throw new IllegalArgumentException(NO_OCCURRENCES_MESSAGE);
        }
    }

    @Override
    /* See https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io/WritableComparable.html*/ public int hashCode()
    {
        return Objects.hash(firstUser, secondUser);
    }

    @Override public int compareTo(@NonNull Contact<?, ?> o)
    {
        return occurrences.first().compareTo(o.getOccurrences().first());
    }

    @Override public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (null == obj || getClass() != obj.getClass())
            return false;
        Contact<?, ?> contact = (Contact<?, ?>) obj;
        boolean withSameOrder = firstUser.equals(contact.getFirstUser()) && secondUser.equals(contact.getSecondUser());
        boolean withDiffOrder = firstUser.equals(contact.getSecondUser()) && secondUser.equals(contact.getFirstUser());
        return withSameOrder || withDiffOrder;
    }
}
