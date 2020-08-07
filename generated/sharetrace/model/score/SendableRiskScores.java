package sharetrace.model.score;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.Objects;
import java.util.SortedSet;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import org.immutables.value.Generated;
import sharetrace.model.identity.UserId;

/**
 * Immutable implementation of {@link AbstractSendableRiskScores}.
 * <p>
 * Use the builder to create immutable instances:
 * {@code SendableRiskScores.builder()}.
 * Use the static factory method to create immutable instances:
 * {@code SendableRiskScores.of()}.
 */
@Generated(from = "AbstractSendableRiskScores", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class SendableRiskScores extends AbstractSendableRiskScores {
  private final ImmutableSortedSet<UserId> sender;
  private final ImmutableSortedSet<RiskScore> message;
  private transient final int hashCode;

  private SendableRiskScores(
      Iterable<? extends UserId> sender,
      Iterable<? extends RiskScore> message) {
    this.sender = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        sender);
    this.message = ImmutableSortedSet.copyOf(
        Ordering.<RiskScore>natural(),
        message);
    this.hashCode = computeHashCode();
  }

  private SendableRiskScores(
      SendableRiskScores original,
      ImmutableSortedSet<UserId> sender,
      ImmutableSortedSet<RiskScore> message) {
    this.sender = sender;
    this.message = message;
    this.hashCode = computeHashCode();
  }

  /**
   * @return The value of the {@code sender} attribute
   */
  @JsonProperty("sender")
  @Override
  public ImmutableSortedSet<UserId> getSender() {
    return sender;
  }

  /**
   * @return The value of the {@code message} attribute
   */
  @JsonProperty("message")
  @Override
  public ImmutableSortedSet<RiskScore> getMessage() {
    return message;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractSendableRiskScores#getSender() sender}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final SendableRiskScores withSender(UserId... elements) {
    ImmutableSortedSet<UserId> newValue = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        Arrays.asList(elements));
    return validate(new SendableRiskScores(this, newValue, this.message));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractSendableRiskScores#getSender() sender}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of sender elements to set
   * @return A modified copy of {@code this} object
   */
  public final SendableRiskScores withSender(Iterable<? extends UserId> elements) {
    if (this.sender == elements) return this;
    ImmutableSortedSet<UserId> newValue = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        elements);
    return validate(new SendableRiskScores(this, newValue, this.message));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractSendableRiskScores#getMessage() message}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final SendableRiskScores withMessage(RiskScore... elements) {
    ImmutableSortedSet<RiskScore> newValue = ImmutableSortedSet.copyOf(
        Ordering.<RiskScore>natural(),
        Arrays.asList(elements));
    return validate(new SendableRiskScores(this, this.sender, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractSendableRiskScores#getMessage() message}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of message elements to set
   * @return A modified copy of {@code this} object
   */
  public final SendableRiskScores withMessage(Iterable<? extends RiskScore> elements) {
    if (this.message == elements) return this;
    ImmutableSortedSet<RiskScore> newValue = ImmutableSortedSet.copyOf(
        Ordering.<RiskScore>natural(),
        elements);
    return validate(new SendableRiskScores(this, this.sender, newValue));
  }

  /**
   * This instance is equal to all instances of {@code SendableRiskScores} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof SendableRiskScores
        && equalTo((SendableRiskScores) another);
  }

  private boolean equalTo(SendableRiskScores another) {
    if (hashCode != another.hashCode) return false;
    return sender.equals(another.sender)
        && message.equals(another.message);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code sender}, {@code message}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    @Var int h = 5381;
    h += (h << 5) + sender.hashCode();
    h += (h << 5) + message.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code SendableRiskScores} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("SendableRiskScores")
        .omitNullValues()
        .add("sender", sender)
        .add("message", message)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractSendableRiskScores", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractSendableRiskScores {
    @Nullable SortedSet<UserId> sender = ImmutableSortedSet.of();
    @Nullable SortedSet<RiskScore> message = ImmutableSortedSet.of();
    @JsonProperty("sender")
    public void setSender(SortedSet<UserId> sender) {
      this.sender = sender;
    }
    @JsonProperty("message")
    public void setMessage(SortedSet<RiskScore> message) {
      this.message = message;
    }
    @Override
    public SortedSet<UserId> getSender() { throw new UnsupportedOperationException(); }
    @Override
    public SortedSet<RiskScore> getMessage() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static SendableRiskScores fromJson(Json json) {
    SendableRiskScores.Builder builder = SendableRiskScores.builder();
    if (json.sender != null) {
      builder.addAllSender(json.sender);
    }
    if (json.message != null) {
      builder.addAllMessage(json.message);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code SendableRiskScores} instance.
   * @param sender The value for the {@code sender} attribute
   * @param message The value for the {@code message} attribute
   * @return An immutable SendableRiskScores instance
   */
  public static SendableRiskScores of(SortedSet<UserId> sender, SortedSet<RiskScore> message) {
    return of((Iterable<? extends UserId>) sender, (Iterable<? extends RiskScore>) message);
  }

  /**
   * Construct a new immutable {@code SendableRiskScores} instance.
   * @param sender The value for the {@code sender} attribute
   * @param message The value for the {@code message} attribute
   * @return An immutable SendableRiskScores instance
   */
  public static SendableRiskScores of(Iterable<? extends UserId> sender, Iterable<? extends RiskScore> message) {
    return validate(new SendableRiskScores(sender, message));
  }

  private static SendableRiskScores validate(SendableRiskScores instance) {
    instance.verifyInputArguments();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AbstractSendableRiskScores} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable SendableRiskScores instance
   */
  public static SendableRiskScores copyOf(AbstractSendableRiskScores instance) {
    if (instance instanceof SendableRiskScores) {
      return (SendableRiskScores) instance;
    }
    return SendableRiskScores.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link SendableRiskScores SendableRiskScores}.
   * <pre>
   * SendableRiskScores.builder()
   *    .addSender|addAllSender(sharetrace.model.identity.UserId) // {@link AbstractSendableRiskScores#getSender() sender} elements
   *    .addMessage|addAllMessage(sharetrace.model.score.RiskScore) // {@link AbstractSendableRiskScores#getMessage() message} elements
   *    .build();
   * </pre>
   * @return A new SendableRiskScores builder
   */
  public static SendableRiskScores.Builder builder() {
    return new SendableRiskScores.Builder();
  }

  /**
   * Builds instances of type {@link SendableRiskScores SendableRiskScores}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractSendableRiskScores", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private ImmutableSortedSet.Builder<UserId> sender = ImmutableSortedSet.naturalOrder();
    private ImmutableSortedSet.Builder<RiskScore> message = ImmutableSortedSet.naturalOrder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractSendableRiskScores} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractSendableRiskScores instance) {
      Objects.requireNonNull(instance, "instance");
      addAllSender(instance.getSender());
      addAllMessage(instance.getMessage());
      return this;
    }

    /**
     * Adds one element to {@link AbstractSendableRiskScores#getSender() sender} sortedSet.
     * @param element A sender element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addSender(UserId element) {
      this.sender.add(element);
      return this;
    }

    /**
     * Adds elements to {@link AbstractSendableRiskScores#getSender() sender} sortedSet.
     * @param elements An array of sender elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addSender(UserId... elements) {
      this.sender.addAll(Arrays.asList(elements));
      return this;
    }


    /**
     * Sets or replaces all elements for {@link AbstractSendableRiskScores#getSender() sender} sortedSet.
     * @param elements An iterable of sender elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("sender")
    public final Builder setSender(Iterable<? extends UserId> elements) {
      this.sender = ImmutableSortedSet.naturalOrder();
      return addAllSender(elements);
    }

    /**
     * Adds elements to {@link AbstractSendableRiskScores#getSender() sender} sortedSet.
     * @param elements An iterable of sender elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllSender(Iterable<? extends UserId> elements) {
      this.sender.addAll(elements);
      return this;
    }

    /**
     * Adds one element to {@link AbstractSendableRiskScores#getMessage() message} sortedSet.
     * @param element A message element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addMessage(RiskScore element) {
      this.message.add(element);
      return this;
    }

    /**
     * Adds elements to {@link AbstractSendableRiskScores#getMessage() message} sortedSet.
     * @param elements An array of message elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addMessage(RiskScore... elements) {
      this.message.addAll(Arrays.asList(elements));
      return this;
    }


    /**
     * Sets or replaces all elements for {@link AbstractSendableRiskScores#getMessage() message} sortedSet.
     * @param elements An iterable of message elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("message")
    public final Builder setMessage(Iterable<? extends RiskScore> elements) {
      this.message = ImmutableSortedSet.naturalOrder();
      return addAllMessage(elements);
    }

    /**
     * Adds elements to {@link AbstractSendableRiskScores#getMessage() message} sortedSet.
     * @param elements An iterable of message elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllMessage(Iterable<? extends RiskScore> elements) {
      this.message.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link SendableRiskScores SendableRiskScores}.
     * @return An immutable instance of SendableRiskScores
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public SendableRiskScores build() {
      return SendableRiskScores.validate(new SendableRiskScores(null, sender.build(), message.build()));
    }
  }
}
