package main.java.model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.Duration;
import java.time.Instant;

/**
 * An occurrence at specific point in time and of a certain duration.
 *
 * The default implementation of compareTo(TemporalOccurrence) is by the point in time.
 * @see Contact
 */
@Value @Builder public class TemporalOccurrence implements Comparable<TemporalOccurrence>
{
    @NonNull Instant time;
    @NonNull Duration duration;

    @Override public int compareTo(TemporalOccurrence temporalOccurrence)
    {
        return getTime().compareTo(temporalOccurrence.getTime());
    }
}
