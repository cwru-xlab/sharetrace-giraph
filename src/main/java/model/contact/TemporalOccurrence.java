package model.contact;

import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import java.time.Duration;
import java.time.Instant;

/**
 * An occurrence at specific point in time and of a certain duration.
 * <p>
 * The default implementation of {@link #compareTo(TemporalOccurrence)} is to first compare {@link #time}. If the
 * occurrences are equally comparable based on the former, {@link #duration} is then used for comparison.
 *
 * @see Contact
 */
@Log4j2
@Value(staticConstructor = "of")
public class TemporalOccurrence implements Comparable<TemporalOccurrence>
{
    @NonNull
    Instant time;

    @NonNull
    Duration duration;

    @Override
    public int compareTo(@NonNull TemporalOccurrence o)
    {
        int compare = time.compareTo(o.getTime());
        if (0 == compare)
        {
            compare = duration.compareTo(o.getDuration());
        }
        return compare;
    }
}
