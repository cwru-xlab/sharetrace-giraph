package sharetrace.algorithm.format.vertex;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.score.SendableRiskScores;

/**
 * Immutable implementation of {@link AbstractVariableVertex}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code VariableVertex.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code VariableVertex.of()}.
 */
@Generated(from = "AbstractVariableVertex", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class VariableVertex extends AbstractVariableVertex {
  private final UserGroup vertexId;
  private final SendableRiskScores vertexValue;
  private transient final int hashCode;

  private VariableVertex(UserGroup vertexId, SendableRiskScores vertexValue) {
    this.vertexId = Objects.requireNonNull(vertexId, "vertexId");
    this.vertexValue = Objects.requireNonNull(vertexValue, "vertexValue");
    this.hashCode = computeHashCode();
  }

  private VariableVertex(
      VariableVertex original,
      UserGroup vertexId,
      SendableRiskScores vertexValue) {
    this.vertexId = vertexId;
    this.vertexValue = vertexValue;
    this.hashCode = computeHashCode();
  }

  /**
   * @return The value of the {@code vertexId} attribute
   */
  @JsonProperty("vertexId")
  @Override
  public UserGroup getVertexId() {
    return vertexId;
  }

  /**
   * @return The value of the {@code vertexValue} attribute
   */
  @JsonProperty("vertexValue")
  @Override
  public SendableRiskScores getVertexValue() {
    return vertexValue;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractVariableVertex#getVertexId() vertexId} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for vertexId
   * @return A modified copy of the {@code this} object
   */
  public final VariableVertex withVertexId(UserGroup value) {
    if (this.vertexId == value) return this;
    UserGroup newValue = Objects.requireNonNull(value, "vertexId");
    return new VariableVertex(this, newValue, this.vertexValue);
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractVariableVertex#getVertexValue() vertexValue} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for vertexValue
   * @return A modified copy of the {@code this} object
   */
  public final VariableVertex withVertexValue(SendableRiskScores value) {
    if (this.vertexValue == value) return this;
    SendableRiskScores newValue = Objects.requireNonNull(value, "vertexValue");
    return new VariableVertex(this, this.vertexId, newValue);
  }

  /**
   * This instance is equal to all instances of {@code VariableVertex} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof VariableVertex
        && equalTo((VariableVertex) another);
  }

  private boolean equalTo(VariableVertex another) {
    if (hashCode != another.hashCode) return false;
    return vertexId.equals(another.vertexId)
        && vertexValue.equals(another.vertexValue);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code vertexId}, {@code vertexValue}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    @Var int h = 5381;
    h += (h << 5) + vertexId.hashCode();
    h += (h << 5) + vertexValue.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code VariableVertex} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("VariableVertex")
        .omitNullValues()
        .add("vertexId", vertexId)
        .add("vertexValue", vertexValue)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractVariableVertex", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractVariableVertex {
    @Nullable UserGroup vertexId;
    @Nullable SendableRiskScores vertexValue;
    @JsonProperty("vertexId")
    public void setVertexId(UserGroup vertexId) {
      this.vertexId = vertexId;
    }
    @JsonProperty("vertexValue")
    public void setVertexValue(SendableRiskScores vertexValue) {
      this.vertexValue = vertexValue;
    }
    @Override
    public UserGroup getVertexId() { throw new UnsupportedOperationException(); }
    @Override
    public SendableRiskScores getVertexValue() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static VariableVertex fromJson(Json json) {
    VariableVertex.Builder builder = VariableVertex.builder();
    if (json.vertexId != null) {
      builder.setVertexId(json.vertexId);
    }
    if (json.vertexValue != null) {
      builder.setVertexValue(json.vertexValue);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code VariableVertex} instance.
   * @param vertexId The value for the {@code vertexId} attribute
   * @param vertexValue The value for the {@code vertexValue} attribute
   * @return An immutable VariableVertex instance
   */
  public static VariableVertex of(UserGroup vertexId, SendableRiskScores vertexValue) {
    return new VariableVertex(vertexId, vertexValue);
  }

  /**
   * Creates an immutable copy of a {@link AbstractVariableVertex} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable VariableVertex instance
   */
  public static VariableVertex copyOf(AbstractVariableVertex instance) {
    if (instance instanceof VariableVertex) {
      return (VariableVertex) instance;
    }
    return VariableVertex.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link VariableVertex VariableVertex}.
   * <pre>
   * VariableVertex.builder()
   *    .setVertexId(sharetrace.model.identity.UserGroup) // required {@link AbstractVariableVertex#getVertexId() vertexId}
   *    .setVertexValue(sharetrace.model.score.SendableRiskScores) // required {@link AbstractVariableVertex#getVertexValue() vertexValue}
   *    .build();
   * </pre>
   * @return A new VariableVertex builder
   */
  public static VariableVertex.Builder builder() {
    return new VariableVertex.Builder();
  }

  /**
   * Builds instances of type {@link VariableVertex VariableVertex}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractVariableVertex", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_VERTEX_ID = 0x1L;
    private static final long INIT_BIT_VERTEX_VALUE = 0x2L;
    private long initBits = 0x3L;

    private @Nullable UserGroup vertexId;
    private @Nullable SendableRiskScores vertexValue;

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractVariableVertex} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractVariableVertex instance) {
      Objects.requireNonNull(instance, "instance");
      setVertexId(instance.getVertexId());
      setVertexValue(instance.getVertexValue());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractVariableVertex#getVertexId() vertexId} attribute.
     * @param vertexId The value for vertexId 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("vertexId")
    public final Builder setVertexId(UserGroup vertexId) {
      this.vertexId = Objects.requireNonNull(vertexId, "vertexId");
      initBits &= ~INIT_BIT_VERTEX_ID;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractVariableVertex#getVertexValue() vertexValue} attribute.
     * @param vertexValue The value for vertexValue 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("vertexValue")
    public final Builder setVertexValue(SendableRiskScores vertexValue) {
      this.vertexValue = Objects.requireNonNull(vertexValue, "vertexValue");
      initBits &= ~INIT_BIT_VERTEX_VALUE;
      return this;
    }

    /**
     * Builds a new {@link VariableVertex VariableVertex}.
     * @return An immutable instance of VariableVertex
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public VariableVertex build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return new VariableVertex(null, vertexId, vertexValue);
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_VERTEX_ID) != 0) attributes.add("vertexId");
      if ((initBits & INIT_BIT_VERTEX_VALUE) != 0) attributes.add("vertexValue");
      return "Cannot build VariableVertex, some of required attributes are not set " + attributes;
    }
  }
}
