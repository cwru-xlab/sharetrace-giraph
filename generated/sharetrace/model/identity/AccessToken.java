package sharetrace.model.identity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import com.google.errorprone.annotations.Var;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import org.immutables.value.Generated;

/**
 * A generic access token intended to be used for writing data to an authenticated entity.
 * @see Identifiable
 */
@Generated(from = "AbstractAccessToken", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class AccessToken extends AbstractAccessToken {
  private final String id;
  private transient final int hashCode;

  private AccessToken(String id) {
    this.id = Objects.requireNonNull(id, "id");
    this.hashCode = computeHashCode();
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
   * This instance is equal to all instances of {@code AccessToken} that have equal attribute values.
   * @return {@code true} if {@code this} is equal to {@code another} instance
   */
  @Override
  public boolean equals(@Nullable Object another) {
    if (this == another) return true;
    return another instanceof AccessToken
        && equalTo((AccessToken) another);
  }

  private boolean equalTo(AccessToken another) {
    if (hashCode != another.hashCode) return false;
    return id.equals(another.id);
  }

  /**
   * Returns a precomputed-on-construction hash code from attributes: {@code id}.
   * @return hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  private int computeHashCode() {
    @Var int h = 5381;
    h += (h << 5) + id.hashCode();
    return h;
  }

  /**
   * Prints the immutable value {@code AccessToken} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("AccessToken")
        .omitNullValues()
        .add("id", id)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractAccessToken", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractAccessToken {
    @Nullable String id;
    @JsonProperty("id")
    public void setId(String id) {
      this.id = id;
    }
    @Override
    public String getId() { throw new UnsupportedOperationException(); }
  }

  /**
   * @param json A JSON-bindable data structure
   * @return An immutable value type
   * @deprecated Do not use this method directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Deprecated
  @JsonCreator(mode = JsonCreator.Mode.DELEGATING)
  static AccessToken fromJson(Json json) {
    AccessToken instance = AccessToken.of(json.id);
    return instance;
  }

  /**
   * Construct a new immutable {@code AccessToken} instance.
   * @param id The value for the {@code id} attribute
   * @return An immutable AccessToken instance
   */
  public static AccessToken of(String id) {
    return validate(new AccessToken(id));
  }

  private static AccessToken validate(AccessToken instance) {
    instance.verifyInputArguments();
    return instance;
  }
}
