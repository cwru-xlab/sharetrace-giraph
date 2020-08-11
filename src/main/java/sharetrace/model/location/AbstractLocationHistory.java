package sharetrace.model.location;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.SortedSet;
import org.immutables.value.Value;
import sharetrace.model.identity.UserId;

/**
 * A collection of {@link TemporalLocation}s that are associated with a {@link UserId}.
 */
@Value.Immutable
@JsonSerialize(as = LocationHistory.class)
@JsonDeserialize(as = LocationHistory.class)
public abstract class AbstractLocationHistory {

  public abstract UserId getId();

  @Value.NaturalOrder
  public abstract SortedSet<TemporalLocation> getHistory();
}
