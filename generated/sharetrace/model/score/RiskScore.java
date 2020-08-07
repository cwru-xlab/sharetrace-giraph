package sharetrace.model.score;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
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
 * A {@link RiskScore} with a time for when it was last updated and an associated user.
 * <p>
 * The default implementation of {@link #compareTo(AbstractRiskScore)} is to first compare risk
 * score update time. If the risk scores are equally comparable based on the former, the value of
 * the risk scores is then used for comparison. Finally, if the risk scores are still equally
 * comparable based on the former two criteria, the {@link #getId()} ()} ()} is used for
 * comparison.
 */
@Generated(from = "AbstractRiskScore", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class RiskScore extends AbstractRiskScore {
  private final String id;
  private final Instant updateTime;
  private final Double value;
  private transient final int hashCode;

  private RiskScore(String id, Instant updateTime, Double value) {
    this.id = Objects.requireNonNull(id, "id");
    this.updateTime = Objects.requireNonNull(updateTime, "updateTime");
    this.value = Objects.requireNonNull(value, "value");
    this.hashCode = super.hashCode();
  }

  private RiskScore(RiskScore original, String id, Instant updateTime, Double value) {
    this.id = id;
    this.updateTime = updateTime;
    this.value = value;
    this.hashCode = super.hashCode();
  }

  /**
   * @return The value of the {@code id} attribute
   */
  @JsonProperty("id")
  @Override
  public String getId() {
    return id;
  }

  /**
   * @return The value of the {@code updateTime} attribute
   */
  @JsonProperty("updateTime")
  @Override
  public Instant getUpdateTime() {
    return updateTime;
  }

  /**
   * @return The value of the {@code value} attribute
   */
  @JsonProperty("value")
  @Override
  public Double getValue() {
    return value;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractRiskScore#getId() id} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for id
   * @return A modified copy of the {@code this} object
   */
  public final RiskScore withId(String value) {
    String newValue = Objects.requireNonNull(value, "id");
    if (this.id.equals(newValue)) return this;
    return validate(new RiskScore(this, newValue, this.updateTime, this.value));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractRiskScore#getUpdateTime() updateTime} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for updateTime
   * @return A modified copy of the {@code this} object
   */
  public final RiskScore withUpdateTime(Instant value) {
    if (this.updateTime == value) return this;
    Instant newValue = Objects.requireNonNull(value, "updateTime");
    return validate(new RiskScore(this, this.id, newValue, this.value));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractRiskScore#getValue() value} attribute.
   * An equals check used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for value
   * @return A modified copy of the {@code this} object
   */
  public final RiskScore withValue(Double value) {
    Double newValue = Objects.requireNonNull(value, "value");
    if (this.value.equals(newValue)) return this;
    return validate(new RiskScore(this, this.id, this.updateTime, newValue));
  }

  /**
   * Returns the precomputed-on-construction hash code from the supertype implementation of {@code super.hashCode()}.
   * @return The hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Prints the immutable value {@code RiskScore} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("RiskScore")
        .omitNullValues()
        .add("id", id)
        .add("updateTime", updateTime)
        .add("value", value)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractRiskScore", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractRiskScore {
    @Nullable String id;
    @Nullable Instant updateTime;
    @Nullable Double value;
    @JsonProperty("id")
    public void setId(String id) {
      this.id = id;
    }
    @JsonProperty("updateTime")
    public void setUpdateTime(Instant updateTime) {
      this.updateTime = updateTime;
    }
    @JsonProperty("value")
    public void setValue(Double value) {
      this.value = value;
    }
    @Override
    public String getId() { throw new UnsupportedOperationException(); }
    @Override
    public Instant getUpdateTime() { throw new UnsupportedOperationException(); }
    @Override
    public Double getValue() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static RiskScore fromJson(Json json) {
    RiskScore.Builder builder = RiskScore.builder();
    if (json.id != null) {
      builder.setId(json.id);
    }
    if (json.updateTime != null) {
      builder.setUpdateTime(json.updateTime);
    }
    if (json.value != null) {
      builder.setValue(json.value);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code RiskScore} instance.
   * @param id The value for the {@code id} attribute
   * @param updateTime The value for the {@code updateTime} attribute
   * @param value The value for the {@code value} attribute
   * @return An immutable RiskScore instance
   */
  public static RiskScore of(String id, Instant updateTime, Double value) {
    return validate(new RiskScore(id, updateTime, value));
  }

  private static RiskScore validate(RiskScore instance) {
    instance.verifyInputArguments();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AbstractRiskScore} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable RiskScore instance
   */
  public static RiskScore copyOf(AbstractRiskScore instance) {
    if (instance instanceof RiskScore) {
      return (RiskScore) instance;
    }
    return RiskScore.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link RiskScore RiskScore}.
   * <pre>
   * RiskScore.builder()
   *    .setId(String) // required {@link AbstractRiskScore#getId() id}
   *    .setUpdateTime(java.time.Instant) // required {@link AbstractRiskScore#getUpdateTime() updateTime}
   *    .setValue(Double) // required {@link AbstractRiskScore#getValue() value}
   *    .build();
   * </pre>
   * @return A new RiskScore builder
   */
  public static RiskScore.Builder builder() {
    return new RiskScore.Builder();
  }

  /**
   * Builds instances of type {@link RiskScore RiskScore}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractRiskScore", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_ID = 0x1L;
    private static final long INIT_BIT_UPDATE_TIME = 0x2L;
    private static final long INIT_BIT_VALUE = 0x4L;
    private long initBits = 0x7L;

    private @Nullable String id;
    private @Nullable Instant updateTime;
    private @Nullable Double value;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code sharetrace.model.score.AbstractRiskScore} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractRiskScore instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    /**
     * Fill a builder with attribute values from the provided {@code sharetrace.model.score.Updatable} instance.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(Updatable instance) {
      Objects.requireNonNull(instance, "instance");
      from((Object) instance);
      return this;
    }

    private void from(Object object) {
      @Var long bits = 0;
      if (object instanceof AbstractRiskScore) {
        AbstractRiskScore instance = (AbstractRiskScore) object;
        setValue(instance.getValue());
        if ((bits & 0x1L) == 0) {
          setUpdateTime(instance.getUpdateTime());
          bits |= 0x1L;
        }
        setId(instance.getId());
      }
      if (object instanceof Updatable) {
        Updatable instance = (Updatable) object;
        if ((bits & 0x1L) == 0) {
          setUpdateTime(instance.getUpdateTime());
          bits |= 0x1L;
        }
      }
    }

    /**
     * Initializes the value for the {@link AbstractRiskScore#getId() id} attribute.
     * @param id The value for id 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("id")
    public final Builder setId(String id) {
      this.id = Objects.requireNonNull(id, "id");
      initBits &= ~INIT_BIT_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractRiskScore#getUpdateTime() updateTime} attribute.
     * @param updateTime The value for updateTime 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("updateTime")
    public final Builder setUpdateTime(Instant updateTime) {
      this.updateTime = Objects.requireNonNull(updateTime, "updateTime");
      initBits &= ~INIT_BIT_UPDATE_TIME;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractRiskScore#getValue() value} attribute.
     * @param value The value for value 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("value")
    public final Builder setValue(Double value) {
      this.value = Objects.requireNonNull(value, "value");
      initBits &= ~INIT_BIT_VALUE;
      return this;
    }

    /**
     * Builds a new {@link RiskScore RiskScore}.
     * @return An immutable instance of RiskScore
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public RiskScore build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return RiskScore.validate(new RiskScore(null, id, updateTime, value));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_ID) != 0) attributes.add("id");
      if ((initBits & INIT_BIT_UPDATE_TIME) != 0) attributes.add("updateTime");
      if ((initBits & INIT_BIT_VALUE) != 0) attributes.add("value");
      return "Cannot build RiskScore, some of required attributes are not set " + attributes;
    }
  }
}
