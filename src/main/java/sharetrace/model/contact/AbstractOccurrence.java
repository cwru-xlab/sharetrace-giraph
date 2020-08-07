package sharetrace.model.contact;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.time.Duration;
import java.time.Instant;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An occurrence at specific point in time and of a certain duration.
 * <p>
 * The default implementation of {@link #compareTo(AbstractOccurrence)} is to first compare {@link
 * #getTime()}. If the occurrences are equally comparable based on the former, {@link
 * #getDuration()} is then used for comparison.
 */
@Value.Immutable
@JsonSerialize(as = Occurrence.class)
@JsonDeserialize(as = Occurrence.class)
public abstract class AbstractOccurrence implements Comparable<AbstractOccurrence> {

  private static final Logger log = LoggerFactory.getLogger(AbstractOccurrence.class);

  public abstract Instant getTime();

  public abstract Duration getDuration();

  @Override
  public final int compareTo(AbstractOccurrence o) {
    Preconditions.checkNotNull(o);
    int compare = getTime().compareTo(o.getTime());
    if (0 == compare) {
      compare = getDuration().compareTo(o.getDuration());
    }
    return compare;
  }
}
