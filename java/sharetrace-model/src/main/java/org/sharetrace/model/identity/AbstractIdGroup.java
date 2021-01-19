package org.sharetrace.model.identity;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of ids.
 */
@Value.Immutable
@JsonSerialize(as = IdGroup.class)
@JsonDeserialize(as = IdGroup.class)
public abstract class AbstractIdGroup {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIdGroup.class);

  private static final String NO_USERS_MESSAGE = "Must contain at least one entry";

  @Value.NaturalOrder
  public abstract SortedSet<String> getIds();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkState(!getIds().isEmpty(), NO_USERS_MESSAGE);
    getIds().forEach(s -> Preconditions.checkArgument(!Strings.isNullOrEmpty(s)));
  }
}
