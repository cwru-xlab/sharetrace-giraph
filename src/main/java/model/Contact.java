package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.Collection;
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

    public boolean addAll(Collection<TemporalOccurrence> temporalOccurrences)
    {
        return occurrences.addAll(temporalOccurrences);
    }

    public boolean add(TemporalOccurrence occurrence) throws IllegalArgumentException
    {
        verifyAdd(occurrence);
        return occurrences.add(occurrence);
    }

    private void verifyAdd(TemporalOccurrence occurrence) throws IllegalArgumentException
    {
        if (occurrences.contains(occurrence))
        {
            throw new IllegalArgumentException(ILLEGAL_TEMPORAL_OCCURRENCE_MESSAGE);
        }
    }

    public boolean removeAll(Collection<TemporalOccurrence> occurrences)
    {
        return occurrences.removeAll(occurrences);
    }

    public boolean remove(TemporalOccurrence occurrence) throws IllegalStateException
    {
        verifyRemove(occurrence);
        return occurrences.remove(occurrence);
    }

    private void verifyRemove(TemporalOccurrence occurrence) throws IllegalStateException
    {
        if (hasSingleOccurrence() && occurrences.contains(occurrence))
        {
            throw new IllegalStateException(ILLEGAL_REMOVAL_MESSAGE);
        }
    }

    private boolean hasSingleOccurrence()
    {
        return 1 == occurrences.size();
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
