package sharetrace.model.contact;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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
 * An encounter between two users at one or more points in time.
 * <p>
 * Two contacts are considered equal if their contacts are equal, regardless of the field they are
 * assigned. That is, a {@link Contact} with {@link #getFirstUser} {@code u1} and {@link
 * #getSecondUser} {@code u2} is considered equal to a different {@link Contact} with equal {@code
 * u1} and {@code u2} except assigned to the opposite field.
 */
@Generated(from = "AbstractContact", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class Contact extends AbstractContact {
  private final UserId firstUser;
  private final UserId secondUser;
  private final ImmutableSortedSet<Occurrence> occurrences;
  private transient final int hashCode;

  private Contact(
      UserId firstUser,
      UserId secondUser,
      Iterable<? extends Occurrence> occurrences) {
    this.firstUser = Objects.requireNonNull(firstUser, "firstUser");
    this.secondUser = Objects.requireNonNull(secondUser, "secondUser");
    this.occurrences = ImmutableSortedSet.copyOf(
        Ordering.<Occurrence>natural(),
        occurrences);
    this.hashCode = super.hashCode();
  }

  private Contact(
      Contact original,
      UserId firstUser,
      UserId secondUser,
      ImmutableSortedSet<Occurrence> occurrences) {
    this.firstUser = firstUser;
    this.secondUser = secondUser;
    this.occurrences = occurrences;
    this.hashCode = super.hashCode();
  }

  /**
   * @return The value of the {@code firstUser} attribute
   */
  @JsonProperty("firstUser")
  @Override
  public UserId getFirstUser() {
    return firstUser;
  }

  /**
   * @return The value of the {@code secondUser} attribute
   */
  @JsonProperty("secondUser")
  @Override
  public UserId getSecondUser() {
    return secondUser;
  }

  /**
   * @return The value of the {@code occurrences} attribute
   */
  @JsonProperty("occurrences")
  @Override
  public ImmutableSortedSet<Occurrence> getOccurrences() {
    return occurrences;
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractContact#getFirstUser() firstUser} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for firstUser
   * @return A modified copy of the {@code this} object
   */
  public final Contact withFirstUser(UserId value) {
    if (this.firstUser == value) return this;
    UserId newValue = Objects.requireNonNull(value, "firstUser");
    return validate(new Contact(this, newValue, this.secondUser, this.occurrences));
  }

  /**
   * Copy the current immutable object by setting a value for the {@link AbstractContact#getSecondUser() secondUser} attribute.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param value A new value for secondUser
   * @return A modified copy of the {@code this} object
   */
  public final Contact withSecondUser(UserId value) {
    if (this.secondUser == value) return this;
    UserId newValue = Objects.requireNonNull(value, "secondUser");
    return validate(new Contact(this, this.firstUser, newValue, this.occurrences));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractContact#getOccurrences() occurrences}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final Contact withOccurrences(Occurrence... elements) {
    ImmutableSortedSet<Occurrence> newValue = ImmutableSortedSet.copyOf(
        Ordering.<Occurrence>natural(),
        Arrays.asList(elements));
    return validate(new Contact(this, this.firstUser, this.secondUser, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractContact#getOccurrences() occurrences}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of occurrences elements to set
   * @return A modified copy of {@code this} object
   */
  public final Contact withOccurrences(Iterable<? extends Occurrence> elements) {
    if (this.occurrences == elements) return this;
    ImmutableSortedSet<Occurrence> newValue = ImmutableSortedSet.copyOf(
        Ordering.<Occurrence>natural(),
        elements);
    return validate(new Contact(this, this.firstUser, this.secondUser, newValue));
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
   * Prints the immutable value {@code Contact} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("Contact")
        .omitNullValues()
        .add("firstUser", firstUser)
        .add("secondUser", secondUser)
        .add("occurrences", occurrences)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractContact", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractContact {
    @Nullable UserId firstUser;
    @Nullable UserId secondUser;
    @Nullable SortedSet<Occurrence> occurrences = ImmutableSortedSet.of();
    @JsonProperty("firstUser")
    public void setFirstUser(UserId firstUser) {
      this.firstUser = firstUser;
    }
    @JsonProperty("secondUser")
    public void setSecondUser(UserId secondUser) {
      this.secondUser = secondUser;
    }
    @JsonProperty("occurrences")
    public void setOccurrences(SortedSet<Occurrence> occurrences) {
      this.occurrences = occurrences;
    }
    @Override
    public UserId getFirstUser() { throw new UnsupportedOperationException(); }
    @Override
    public UserId getSecondUser() { throw new UnsupportedOperationException(); }
    @Override
    public SortedSet<Occurrence> getOccurrences() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static Contact fromJson(Json json) {
    Contact.Builder builder = Contact.builder();
    if (json.firstUser != null) {
      builder.setFirstUser(json.firstUser);
    }
    if (json.secondUser != null) {
      builder.setSecondUser(json.secondUser);
    }
    if (json.occurrences != null) {
      builder.addAllOccurrences(json.occurrences);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code Contact} instance.
   * @param firstUser The value for the {@code firstUser} attribute
   * @param secondUser The value for the {@code secondUser} attribute
   * @param occurrences The value for the {@code occurrences} attribute
   * @return An immutable Contact instance
   */
  public static Contact of(UserId firstUser, UserId secondUser, SortedSet<Occurrence> occurrences) {
    return of(firstUser, secondUser, (Iterable<? extends Occurrence>) occurrences);
  }

  /**
   * Construct a new immutable {@code Contact} instance.
   * @param firstUser The value for the {@code firstUser} attribute
   * @param secondUser The value for the {@code secondUser} attribute
   * @param occurrences The value for the {@code occurrences} attribute
   * @return An immutable Contact instance
   */
  public static Contact of(UserId firstUser, UserId secondUser, Iterable<? extends Occurrence> occurrences) {
    return validate(new Contact(firstUser, secondUser, occurrences));
  }

  private static Contact validate(Contact instance) {
    instance.verifyInputArguments();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AbstractContact} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable Contact instance
   */
  public static Contact copyOf(AbstractContact instance) {
    if (instance instanceof Contact) {
      return (Contact) instance;
    }
    return Contact.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link Contact Contact}.
   * <pre>
   * Contact.builder()
   *    .setFirstUser(sharetrace.model.identity.UserId) // required {@link AbstractContact#getFirstUser() firstUser}
   *    .setSecondUser(sharetrace.model.identity.UserId) // required {@link AbstractContact#getSecondUser() secondUser}
   *    .addOccurrence|addAllOccurrences(sharetrace.model.contact.Occurrence) // {@link AbstractContact#getOccurrences() occurrences} elements
   *    .build();
   * </pre>
   * @return A new Contact builder
   */
  public static Contact.Builder builder() {
    return new Contact.Builder();
  }

  /**
   * Builds instances of type {@link Contact Contact}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractContact", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private static final long INIT_BIT_FIRST_USER = 0x1L;
    private static final long INIT_BIT_SECOND_USER = 0x2L;
    private long initBits = 0x3L;

    private @Nullable UserId firstUser;
    private @Nullable UserId secondUser;
    private ImmutableSortedSet.Builder<Occurrence> occurrences = ImmutableSortedSet.naturalOrder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractContact} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractContact instance) {
      Objects.requireNonNull(instance, "instance");
      setFirstUser(instance.getFirstUser());
      setSecondUser(instance.getSecondUser());
      addAllOccurrences(instance.getOccurrences());
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractContact#getFirstUser() firstUser} attribute.
     * @param firstUser The value for firstUser 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("firstUser")
    public final Builder setFirstUser(UserId firstUser) {
      this.firstUser = Objects.requireNonNull(firstUser, "firstUser");
      initBits &= ~INIT_BIT_FIRST_USER;
      return this;
    }

    /**
     * Initializes the value for the {@link AbstractContact#getSecondUser() secondUser} attribute.
     * @param secondUser The value for secondUser 
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("secondUser")
    public final Builder setSecondUser(UserId secondUser) {
      this.secondUser = Objects.requireNonNull(secondUser, "secondUser");
      initBits &= ~INIT_BIT_SECOND_USER;
      return this;
    }

    /**
     * Adds one element to {@link AbstractContact#getOccurrences() occurrences} sortedSet.
     * @param element A occurrences element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addOccurrence(Occurrence element) {
      this.occurrences.add(element);
      return this;
    }

    /**
     * Adds elements to {@link AbstractContact#getOccurrences() occurrences} sortedSet.
     * @param elements An array of occurrences elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addOccurrences(Occurrence... elements) {
      this.occurrences.addAll(Arrays.asList(elements));
      return this;
    }


    /**
     * Sets or replaces all elements for {@link AbstractContact#getOccurrences() occurrences} sortedSet.
     * @param elements An iterable of occurrences elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("occurrences")
    public final Builder setOccurrences(Iterable<? extends Occurrence> elements) {
      this.occurrences = ImmutableSortedSet.naturalOrder();
      return addAllOccurrences(elements);
    }

    /**
     * Adds elements to {@link AbstractContact#getOccurrences() occurrences} sortedSet.
     * @param elements An iterable of occurrences elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllOccurrences(Iterable<? extends Occurrence> elements) {
      this.occurrences.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link Contact Contact}.
     * @return An immutable instance of Contact
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public Contact build() {
      if (initBits != 0) {
        throw new IllegalStateException(formatRequiredAttributesMessage());
      }
      return Contact.validate(new Contact(null, firstUser, secondUser, occurrences.build()));
    }

    private String formatRequiredAttributesMessage() {
      List<String> attributes = new ArrayList<>();
      if ((initBits & INIT_BIT_FIRST_USER) != 0) attributes.add("firstUser");
      if ((initBits & INIT_BIT_SECOND_USER) != 0) attributes.add("secondUser");
      return "Cannot build Contact, some of required attributes are not set " + attributes;
    }
  }
}
