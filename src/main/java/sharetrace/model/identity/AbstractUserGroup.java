package sharetrace.model.identity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.common.Wrappable;

/**
 * A collection of {@link UserId}s.
 *
 * @see UserId
 */
@Value.Immutable
@JsonSerialize(as = UserGroup.class)
@JsonDeserialize(as = UserGroup.class)
public abstract class AbstractUserGroup implements Wrappable<UserGroupWritableComparable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractUserGroup.class);

  private static final String NO_USERS_MESSAGE = "UserGroup must contain at least one user";

  @Value.NaturalOrder
  public abstract SortedSet<UserId> getUsers();

  @Override
  public final UserGroupWritableComparable wrap() {
    return UserGroupWritableComparable.of(this);
  }

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkState(!getUsers().isEmpty(), NO_USERS_MESSAGE);
  }
}
