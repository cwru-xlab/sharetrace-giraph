package sharetrace.model.identity;

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

/**
 * A collection of {@link UserId}s.
 * @see UserId
 */
@Generated(from = "AbstractUserGroup", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class UserGroup extends AbstractUserGroup {
  private final ImmutableSortedSet<UserId> users;
  private transient final int hashCode;

  private UserGroup(Iterable<? extends UserId> users) {
    this.users = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        users);
    this.hashCode = computeHashCode();
  }

  private UserGroup(
      UserGroup original,
      ImmutableSortedSet<UserId> users) {
    this.users = users;
    this.hashCode = computeHashCode();
  }

  /**
   * @return The value of the {@code users} attribute
   */
  @JsonProperty("users")
  @Override
  public ImmutableSortedSet<UserId> getUsers() {
    return users;
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractUserGroup#getUsers() users}.
   * @param elements The elements to set
   * @return A modified copy of {@code this} object
   */
  public final UserGroup withUsers(UserId... elements) {
    ImmutableSortedSet<UserId> newValue = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        Arrays.asList(elements));
    return validate(new UserGroup(this, newValue));
  }

  /**
   * Copy the current immutable object with elements that replace the content of {@link AbstractUserGroup#getUsers() users}.
   * A shallow reference equality check is used to prevent copying of the same value by returning {@code this}.
   * @param elements An iterable of users elements to set
   * @return A modified copy of {@code this} object
   */
  public final UserGroup withUsers(Iterable<? extends UserId> elements) {
    if (this.users == elements) return this;
    ImmutableSortedSet<UserId> newValue = ImmutableSortedSet.copyOf(
        Ordering.<UserId>natural(),
        elements);
    return validate(new UserGroup(this, newValue));
  }

  /**
   * This instance is equal to all instances of {@code UserGroup} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof UserGroup
        && equalTo((UserGroup) another);
  }

  private boolean equalTo(UserGroup another) {
    if (hashCode != another.hashCode) return false;
    return users.equals(another.users);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code users}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    @Var int h = 5381;
    h += (h << 5) + users.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code UserGroup} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("UserGroup")
        .omitNullValues()
        .add("users", users)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractUserGroup", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractUserGroup {
    @Nullable SortedSet<UserId> users = ImmutableSortedSet.of();
    @JsonProperty("users")
    public void setUsers(SortedSet<UserId> users) {
      this.users = users;
    }
    @Override
    public SortedSet<UserId> getUsers() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static UserGroup fromJson(Json json) {
    UserGroup.Builder builder = UserGroup.builder();
    if (json.users != null) {
      builder.addAllUsers(json.users);
    }
    return builder.build();
  }

  /**
   * Construct a new immutable {@code UserGroup} instance.
   * @param users The value for the {@code users} attribute
   * @return An immutable UserGroup instance
   */
  public static UserGroup of(SortedSet<UserId> users) {
    return of((Iterable<? extends UserId>) users);
  }

  /**
   * Construct a new immutable {@code UserGroup} instance.
   * @param users The value for the {@code users} attribute
   * @return An immutable UserGroup instance
   */
  public static UserGroup of(Iterable<? extends UserId> users) {
    return validate(new UserGroup(users));
  }

  private static UserGroup validate(UserGroup instance) {
    instance.verifyInputArguments();
    return instance;
  }

  /**
   * Creates an immutable copy of a {@link AbstractUserGroup} value.
   * Uses accessors to get values to initialize the new immutable instance.
   * If an instance is already immutable, it is returned as is.
   * @param instance The instance to copy
   * @return A copied immutable UserGroup instance
   */
  public static UserGroup copyOf(AbstractUserGroup instance) {
    if (instance instanceof UserGroup) {
      return (UserGroup) instance;
    }
    return UserGroup.builder()
        .from(instance)
        .build();
  }

  /**
   * Creates a builder for {@link UserGroup UserGroup}.
   * <pre>
   * UserGroup.builder()
   *    .addUser|addAllUsers(sharetrace.model.identity.UserId) // {@link AbstractUserGroup#getUsers() users} elements
   *    .build();
   * </pre>
   * @return A new UserGroup builder
   */
  public static UserGroup.Builder builder() {
    return new UserGroup.Builder();
  }

  /**
   * Builds instances of type {@link UserGroup UserGroup}.
   * Initialize attributes and then invoke the {@link #build()} method to create an
   * immutable instance.
   * <p><em>{@code Builder} is not thread-safe and generally should not be stored in a field or collection,
   * but instead used immediately to create instances.</em>
   */
  @Generated(from = "AbstractUserGroup", generator = "Immutables")
  @NotThreadSafe
  public static final class Builder {
    private ImmutableSortedSet.Builder<UserId> users = ImmutableSortedSet.naturalOrder();

    private Builder() {
    }

    /**
     * Fill a builder with attribute values from the provided {@code AbstractUserGroup} instance.
     * Regular attribute values will be replaced with those from the given instance.
     * Absent optional values will not replace present values.
     * Collection elements and entries will be added, not replaced.
     * @param instance The instance from which to copy values
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder from(AbstractUserGroup instance) {
      Objects.requireNonNull(instance, "instance");
      addAllUsers(instance.getUsers());
      return this;
    }

    /**
     * Adds one element to {@link AbstractUserGroup#getUsers() users} sortedSet.
     * @param element A users element
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addUser(UserId element) {
      this.users.add(element);
      return this;
    }

    /**
     * Adds elements to {@link AbstractUserGroup#getUsers() users} sortedSet.
     * @param elements An array of users elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addUsers(UserId... elements) {
      this.users.addAll(Arrays.asList(elements));
      return this;
    }


    /**
     * Sets or replaces all elements for {@link AbstractUserGroup#getUsers() users} sortedSet.
     * @param elements An iterable of users elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    @JsonProperty("users")
    public final Builder setUsers(Iterable<? extends UserId> elements) {
      this.users = ImmutableSortedSet.naturalOrder();
      return addAllUsers(elements);
    }

    /**
     * Adds elements to {@link AbstractUserGroup#getUsers() users} sortedSet.
     * @param elements An iterable of users elements
     * @return {@code this} builder for use in a chained invocation
     */
    @CanIgnoreReturnValue 
    public final Builder addAllUsers(Iterable<? extends UserId> elements) {
      this.users.addAll(elements);
      return this;
    }

    /**
     * Builds a new {@link UserGroup UserGroup}.
     * @return An immutable instance of UserGroup
     * @throws java.lang.IllegalStateException if any required attributes are missing
     */
    public UserGroup build() {
      return UserGroup.validate(new UserGroup(null, users.build()));
    }
  }
}
