package sharetrace.model.identity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.MoreObjects;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Objects;
import javax.annotation.CheckReturnValue;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.annotation.concurrent.Immutable;
import org.immutables.value.Generated;

/**
 * An identifier for a user.
 * @see Identifiable
 */
@Generated(from = "AbstractUserId", generator = "Immutables")
@SuppressWarnings({"all"})
@SuppressFBWarnings
@ParametersAreNonnullByDefault
@javax.annotation.processing.Generated("org.immutables.processor.ProxyProcessor")
@Immutable
@CheckReturnValue
public final class UserId extends AbstractUserId {
  private final String id;
  private transient final int hashCode;

  private UserId(String id) {
    this.id = Objects.requireNonNull(id, "id");
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
   * Returns the precomputed-on-construction hash code from the supertype implementation of {@code super.hashCode()}.
   * @return The hashCode value
   */
  @Override
  public int hashCode() {
    return hashCode;
  }

  /**
   * Prints the immutable value {@code UserId} with attribute values.
   * @return A string representation of the value
   */
  @Override
  public String toString() {
    return MoreObjects.toStringHelper("UserId")
        .omitNullValues()
        .add("id", id)
        .toString();
  }

  /**
   * Utility type used to correctly read immutable object from JSON representation.
   * @deprecated Do not use this type directly, it exists only for the <em>Jackson</em>-binding infrastructure
   */
  @Generated(from = "AbstractUserId", generator = "Immutables")
  @Deprecated
  @SuppressWarnings("Immutable")
  @JsonDeserialize
  @JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE)
  static final class Json extends AbstractUserId {
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
  static UserId fromJson(Json json) {
    UserId instance = UserId.of(json.id);
    return instance;
  }

  /**
   * Construct a new immutable {@code UserId} instance.
   * @param id The value for the {@code id} attribute
   * @return An immutable UserId instance
   */
  public static UserId of(String id) {
    return validate(new UserId(id));
  }

  private static UserId validate(UserId instance) {
    instance.verifyInputArguments();
    return instance;
  }
}
