package model.contact;

import lombok.AccessLevel;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import model.identity.Identifiable;
import model.identity.UserId;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * An encounter between two users at one or more points in time.
 * <p>
 * Two contacts are considered equal if their contacts are equal, regardless of the field they are assigned. That is,
 * a {@code Contact} with {@link #firstUser} {@code u1} and {@link #secondUser} {@code u2} is considered equal to a
 * different {@code Contact} with equal {@code u1} and {@code u2} except assigned to the opposite field.
 */
@Log4j2
@Data
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class Contact implements Comparable<Contact>, Writable
{
    private static final String NO_OCCURRENCES_MESSAGE = "A contact must contain at least one occurrence.";

    private static final String IDENTICAL_USERS_MESSAGE = "A contact must contain two distinct users.";

    private UserId firstUser;

    private UserId secondUser;

    private SortedSet<TemporalOccurrence> occurrences;

    private Contact(@NonNull Identifiable<String> firstUser,
                    @NonNull Identifiable<String> secondUser,
                    @NonNull SortedSet<TemporalOccurrence> occurrences)
    {
        verifyUsers(firstUser, secondUser);
        verifyOccurrences(occurrences);
        this.firstUser = UserId.of(firstUser.getId());
        this.secondUser = UserId.of(secondUser.getId());
        this.occurrences = Collections.unmodifiableSortedSet(occurrences);
    }

    private static void verifyUsers(Identifiable<String> user1, Identifiable<String> user2)
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

    public static Contact of(@NonNull Identifiable<String> firstUser,
                             @NonNull Identifiable<String> secondUser,
                             @NonNull SortedSet<TemporalOccurrence> occurrences)
    {
        return new Contact(firstUser, secondUser, occurrences);
    }

    @Override
    public final int compareTo(@NonNull Contact o)
    {
        int compare = firstUser.compareTo(o.getFirstUser());
        {
            if (0 == compare)
            {
                compare = secondUser.compareTo(o.getSecondUser());
            }
        }
        return compare;
    }

    @Override
    public final boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (null == o || getClass() != o.getClass())
            return false;
        Contact contact = (Contact) o;
        boolean withSameOrder = firstUser.equals(contact.getFirstUser()) && secondUser.equals(contact.getSecondUser());
        boolean withDiffOrder = firstUser.equals(contact.getSecondUser()) && secondUser.equals(contact.getFirstUser());
        return withSameOrder || withDiffOrder;
    }

    @Override
    public final int hashCode()
    {
        return Objects.hash(firstUser.getId(), secondUser.getId());
    }

    @Override
    public final void write(DataOutput dataOutput) throws IOException
    {
        firstUser.write(dataOutput);
        secondUser.write(dataOutput);
        for (TemporalOccurrence occurrence : occurrences)
        {
            dataOutput.writeLong(occurrence.getTime().toEpochMilli());
            dataOutput.writeLong(occurrence.getDuration().toMillis());
        }
    }

    @Override
    public final void readFields(DataInput dataInput) throws IOException
    {
        setFirstUser(UserId.of(dataInput.readUTF()));
        setSecondUser(UserId.of(dataInput.readUTF()));
        int nOccurrences = dataInput.readInt();
        SortedSet<TemporalOccurrence> temporalOccurrences = new TreeSet<>();
        for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++)
        {
            temporalOccurrences.add(TemporalOccurrence.of(Instant.ofEpochMilli(dataInput.readLong()),
                                                          Duration.ofMillis(dataInput.readLong())));
        }
        setOccurrences(temporalOccurrences);
    }

    private void setFirstUser(Identifiable<String> user)
    {
        verifyUsers(user, secondUser);
        firstUser = UserId.of(user.getId());
    }

    private void setSecondUser(Identifiable<String> user)
    {
        verifyUsers(user, firstUser);
        secondUser = UserId.of(user.getId());
    }

    private void setOccurrences(SortedSet<TemporalOccurrence> temporalOccurrences)
    {
        verifyOccurrences(temporalOccurrences);
        occurrences = Collections.unmodifiableSortedSet(temporalOccurrences);
    }
}
