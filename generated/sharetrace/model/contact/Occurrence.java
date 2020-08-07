package sharetrace.model.contact;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;

/**
 * An occurrence at specific point in time and of a certain duration.
 * <p>
 * The default implementation of {@link #compareTo(AbstractOccurrence)} is to first compare {@link
 * #getTime()}. If the occurrences are equally comparable based on the former, {@link
 * #getDuration()} is then used for comparison.
 */
@Generated(from = "AbstractOccurrence", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class Occurrence extends AbstractOccurrence {
  private final Instant time;
  private final Duration duration;
  private transient final int hashCode;

  private Occurrence(Instant time, Duration duration) {
    this.time = Objects.requireNonNull(time, "time");
    this.duration = Objects.requireNonNull(duration, "duration");
    this.hashCode = computeHashCode();
  }

  private Occurrence(Occurrence original, Instant time, Duration duration) {
    this.time = time;
    this.duration = duration;
    this.hashCode = computeHashCode();
  }

  /**
   * @return The value of the {@code time} attribute
   */
  @JsonProperty("time")
  @Override
  public Instant getTime() {
    return time;
  }

  /**
   * @return The value of the {@code duration} attribute
   */
  @JsonProperty("duration")
  @Override
  public Duration getDuration() {
    return duration;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractOccurrence#getTime() time} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for time
   * @return A modified copy of the {@code this} object
   */
  public final Occurrence withTime(Instant value) {
    if (this.time == value) return this;
    Instant newValue = Objects.requireNonNull(value, "time");
    return new Occurrence(this, newValue, this.duration);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractOccurrence#getDuration() duration} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for duration
   * @return A modified copy of the {@code this} object
   */
  public final Occurrence withDuration(Duration value) {
    if (this.duration == value) return this;
    Duration newValue = Objects.requireNonNull(value, "duration");
    return new Occurrence(this, this.time, newValue);
  }

  /**
   * This instance is equal to all instances of {@code Occurrence} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof Occurrence
        && equalTo((Occurrence) another);
  }

  private boolean equalTo(Occurrence another) {
    if (hashCode != another.hashCode) return false;
    return time.equals(another.time)
        && duration.equals(another.duration);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code time}, {@code duration}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    @Var int h = 5381;
    h += (h << 5) + time.hashCode();
    h += (h << 5) + duration.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code Occurrence} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Occurrence")
        .omitNullValues()
        .add("time", time)
        .add("duration", duration)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractOccurrence", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractOccurrence {
    @Nullable Instant time;
    @Nullable Duration duration;
    @JsonProperty("time")
    public void setTime(Instant time) {
      this.time = time;
    }
    @JsonProperty("duration")
    public void setDuration(Duration duration) {
      this.duration = duration;
    }
    @Override
    public Instant getTime() { throw new UnsupportedOperationException(); }
    @Override
    public Duration getDuration() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static Occurrence fromJson(Json json) {
    Occurrence.Builder builder = Occurrence.builder();
    if (json.time != null) {
      builder.setTime(json.time);
    }
    if (json.duration != null) {
      builder.setDuration(json.duration);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code Occurrence} instance.
   * @param time The value for the {@code time} attribute
   * @param duration The value for the {@code duration} attribute
   * @return An immutable Occurrence instance
   */
  public static Occurrence of(Instant time, Duration duration) {
    return new Occurrence(time, duration);
  }

  /**
   * Creates an immutable copy of a {@link AbstractOccurrence} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Occurrence instance
   */
  public static Occurrence copyOf(AbstractOccurrence instance) {
    if (instance instanceof Occurrence) {
      return (Occurrence) instance;
    }
    return Occurrence.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link Occurrence Occurrence}.
   * <pre>
   * Occurrence.builder()
   *    .setTime(java.time.Instant) // required {@link AbstractOccurrence#getTime() time}
   *    .setDuration(java.time.Duration) // required {@link AbstractOccurrence#getDuration() duration}
   *    .build();
   * </pre>
   * @return A new Occurrence builder
   */
  public static Occurrence.Builder builder() {
    return new Occurrence.Builder();
  }

  /**
   * Builds instances of type {@link Occurrence Occurrence}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractOccurrence", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_TIME = 0x1L;
    private static final long INIT_BIT_DURATION = 0x2L;
    private long initBits = 0x3L;

    private @Nullable Instant time;
    private @Nullable Duration duration;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractOccurrence} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractOccurrence instance) {
      Objects.requireNonNull(instance, "instance");
      setTime(instance.getTime());
      setDuration(instance.getDuration());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractOccurrence#getTime() time} attribute.
     * @param time The value for time 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("time")
    public final Builder setTime(Instant time) {
      this.time = Objects.requireNonNull(time, "time");
      initBits &= ~INIT_BIT_TIME;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractOccurrence#getDuration() duration} attribute.
     * @param duration The value for duration 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("duration")
    public final Builder setDuration(Duration duration) {
      this.duration = Objects.requireNonNull(duration, "duration");
      initBits &= ~INIT_BIT_DURATION;
      return this;
    }

    /**
     * Builds a new {@link Occurrence Occurrence}.
     * @return An immutable instance of Occurrence
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public Occurrence build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new Occurrence(null, time, duration);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_TIME) != 0) attributes.add("time");
      if ((initBits & INIT_BIT_DURATION) != 0) attributes.add("duration");
      return "Cannot build Occurrence, some of required attributes are not set " + attributes;
    }
  }
}
