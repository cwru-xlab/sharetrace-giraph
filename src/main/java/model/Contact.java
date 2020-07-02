package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

@Value @Builder public class Contact implements Comparable<Contact>
{
    @NonNull UserID<?> firstUser;
    @NonNull UserID<?> secondUser;
    @NonNull Instant timeOfContact;
    @NonNull Duration durationOfContact;

    public boolean containsUser(UserID<?> userID)
    {
        return userID.equals(getFirstUser()) || userID.equals(getSecondUser());
    }

    @Override
    /* See https://hadoop.apache.org/docs/current/api/org/apache/hadoop/io
    /WritableComparable.html*/ public int hashCode()
    {
        return Objects.hash(getFirstUser(),
                            getSecondUser(),
                            getTimeOfContact(),
                            getDurationOfContact());
    }

    @Override public int compareTo(Contact o)
    {
        return getDurationOfContact().compareTo(o.getDurationOfContact());
    }
}
