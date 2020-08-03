package model.contact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An occurrence at specific point in time and of a certain duration.
 * <p>
 * The default implementation of {@link #compareTo(TemporalOccurrence)} is to first compare {@link
 * #time}. If the occurrences are equally comparable based on the former, {@link #duration} is then
 * used for comparison.
 *
 * @see Contact
 */
public final class TemporalOccurrence implements Writable, Comparable<TemporalOccurrence> {

  private static final Logger log = LoggerFactory.getLogger(TemporalOccurrence.class);

  private static final String TIME_LABEL = "time";

  private static final String DURATION_LABEL = "duration";

  private Instant time;

  private Duration duration;

  @JsonCreator
  private TemporalOccurrence(Instant time, Duration duration) {
    Preconditions.checkNotNull(time);
    Preconditions.checkNotNull(duration);
    this.time = time;
    this.duration = duration;
  }

  private TemporalOccurrence() {
  }

  static TemporalOccurrence fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating a TemporalOccurrence from DataInput");
    TemporalOccurrence occurrence = new TemporalOccurrence();
    occurrence.readFields(dataInput);
    return occurrence;
  }

  public static TemporalOccurrence fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating a TemporalOccurrence from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    long second = jsonNode.get(TIME_LABEL).asLong();
    long duration = jsonNode.get(DURATION_LABEL).asLong();
    return new TemporalOccurrence(Instant.ofEpochSecond(second), Duration.ofSeconds(duration));
  }

  public static TemporalOccurrence of(Instant time, Duration duration) {
    return new TemporalOccurrence(time, duration);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    time = Instant.ofEpochSecond(dataInput.readLong());
    duration = Duration.ofSeconds(dataInput.readLong());
  }

  @Override
  public int compareTo(TemporalOccurrence o) {
    Preconditions.checkNotNull(o);
    int compare = time.compareTo(o.getTime());
    if (0 == compare) {
      compare = duration.compareTo(o.getDuration());
    }
    return compare;
  }

  public Instant getTime() {
    return time;
  }

  public Duration getDuration() {
    return duration;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeLong(time.getEpochSecond());
    dataOutput.writeLong(duration.toSeconds());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    TemporalOccurrence occurrence = (TemporalOccurrence) o;
    boolean equalTime = Objects.equals(time, occurrence.getTime());
    boolean equalDuration = Objects.equals(duration, occurrence.getDuration());
    return equalTime && equalDuration;
  }

  @Override
  public int hashCode() {
    return Objects.hash(time, duration);
  }

  @Override
  public String toString() {
    return MessageFormat.format("TemporalOccurrence'{'time={0}, duration={1}'}'", time,
        duration);
  }
}
