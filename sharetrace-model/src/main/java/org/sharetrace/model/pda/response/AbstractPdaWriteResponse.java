package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Map;
import org.immutables.value.Value;

/**
 * Response to a write request sent to a PDA.
 */
@Value.Immutable
@JsonSerialize(as = PdaWriteResponse.class)
@JsonDeserialize(as = PdaWriteResponse.class)
public abstract class AbstractPdaWriteResponse {

  public abstract Map<String, Object> getData();
}
