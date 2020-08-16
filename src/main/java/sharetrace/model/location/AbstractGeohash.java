package sharetrace.model.location;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.immutables.value.Value;

/**
 * A geohash with a timestamp.
 */
@JsonSerialize(as = Geohash.class)
@JsonDeserialize(as = Geohash.class)
@Value.Immutable
public abstract class AbstractGeohash {

  public abstract String getHash();

  public abstract Instant getTimestamp();
}
