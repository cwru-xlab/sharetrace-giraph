package sharetrace.model.identity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.Objects;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.common.Wrapped;

/**
 * An identifier for a user.
 *
 * @see Identifiable
 */
@Value.Immutable
@Wrapped
@JsonSerialize(as = UserId.class)
@JsonDeserialize(as = UserId.class)
public abstract class AbstractUserId implements Identifiable<String>, Comparable<AbstractUserId> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUserId.class);

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getId()));
  }

  @Override
  public final int compareTo(AbstractUserId o) {
    return getId().compareTo(o.getId());
  }


  @Override
  public final boolean equals(Object o) {
    if (!(o instanceof AbstractUserId)) {
      return false;
    }
    return 0 == compareTo((AbstractUserId) o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getId());
  }
}
