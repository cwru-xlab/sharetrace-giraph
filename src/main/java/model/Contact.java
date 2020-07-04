package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Objects;
import java.util.SortedSet;

@Value @Builder public class Contact<U1, U2> implements Comparable<Contact<?, ?>>
{
    @NonNull Identifiable<U1> firstUser;
    @NonNull Identifiable<U2> secondUser;
    @NonNull SortedSet<TemporalOccurrence> occurrences;
    private static final String ILLEGAL_TEMPORAL_OCCURRENCE_MESSAGE = "Occurrence already exists.";
    private static final String ILLEGAL_REMOVAL_MESSAGE = "A contact must contain at least one occurrence.";

    public boolean containsUser(Identifiable<?> userID)
    {
        return userID.equals(firstUser) || userID.equals(secondUser);
    }

    @Override
    /* See https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io
    /WritableComparable.html*/ public int hashCode()
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
