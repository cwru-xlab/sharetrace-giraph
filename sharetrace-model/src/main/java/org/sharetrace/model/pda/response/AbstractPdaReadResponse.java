package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Response to a read request sent to a PDA.
 */
@Value.Immutable
@JsonSerialize(as = PdaReadResponse.class)
@JsonDeserialize(as = PdaReadResponse.class)
public abstract class AbstractPdaReadResponse {

  public abstract Map<String, Object> getData();
}
