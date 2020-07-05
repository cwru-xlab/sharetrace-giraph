package main.java.model;

import lombok.NonNull;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;

/**
 * An occurrence at specific point in time and of a certain duration.
 * <p>
 * The default implementation of compareTo(TemporalOccurrence) is by the point in time.
 *
 * @see Contact
 */
@Value(staticConstructor = "of")
public class TemporalOccurrence implements Comparable<TemporalOccurrence>
{
    @NonNull
    Instant time;

    @NonNull
    Duration duration;

    @Override
    public int compareTo(TemporalOccurrence o)
    {
        return time.compareTo(o.getTime());
    }
}
