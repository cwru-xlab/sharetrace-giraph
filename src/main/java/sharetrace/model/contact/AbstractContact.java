package sharetrace.model.contact;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.Objects;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.common.Wrappable;
import sharetrace.model.identity.UserId;

/**
 * An encounter between two users at one or more points in time.
 * <p>
 * Two contacts are considered equal if their contacts are equal, regardless of the field they are
 * assigned. That is, a {@link Contact} with {@link #getFirstUser} {@code u1} and {@link
 * #getSecondUser} {@code u2} is considered equal to a different {@link Contact} with equal {@code
 * u1} and {@code u2} except assigned to the opposite field.
 */
@Value.Immutable
@JsonSerialize(as = Contact.class)
@JsonDeserialize(as = Contact.class)
public abstract class AbstractContact implements Wrappable<ContactWritable> {

  private static final Logger log = LoggerFactory.getLogger(AbstractContact.class);

  private static final String NO_OCCURRENCES_MESSAGE = "A contact must contain at least one occurrence";

  private static final String IDENTICAL_USERS_MESSAGE = "A contact must contain two distinct users";

  public abstract UserId getFirstUser();

  public abstract UserId getSecondUser();

  @Value.NaturalOrder
  public abstract SortedSet<Occurrence> getOccurrences();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkState(!getFirstUser().equals(getSecondUser()), IDENTICAL_USERS_MESSAGE);
    Preconditions.checkState(!getOccurrences().isEmpty(), NO_OCCURRENCES_MESSAGE);
  }

  @Override
  public final ContactWritable wrap() {
    return ContactWritable.of(this);
  }

  @Override
  public final boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    AbstractContact contact = (AbstractContact) o;
    boolean firstEqualToFirst = getFirstUser().equals(contact.getFirstUser());
    boolean secondEqualToSecond = getSecondUser().equals(contact.getSecondUser());
    boolean firstEqualToSecond = getFirstUser().equals(contact.getSecondUser());
    boolean secondEqualToFirst = getSecondUser().equals(contact.getFirstUser());
    boolean withSameOrder = firstEqualToFirst && secondEqualToSecond;
    boolean withDiffOrder = firstEqualToSecond && secondEqualToFirst;
    return withSameOrder || withDiffOrder;
  }

  // Leave as non-final since Immutables cannot override it.
  @Override
  public int hashCode() {
    return Objects.hash(getFirstUser(), getSecondUser());
  }
}
