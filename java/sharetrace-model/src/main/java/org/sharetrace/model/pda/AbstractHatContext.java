package org.sharetrace.model.pda;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonProperty.Access;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = HatContext.class)
@JsonDeserialize(as = HatContext.class)
public abstract class AbstractHatContext {

  private static final String INVALID_HAT_MSG = "Hat must not be empty String or null";

  @JsonProperty(value = "hatName", access = Access.READ_WRITE)
  public abstract String getHatName();

  @Value.Default
  public int getNumRecordsRead() {
    return 0;
  }

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getHatName()), INVALID_HAT_MSG);
  }

}
