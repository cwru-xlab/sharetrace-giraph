package sharetrace.algorithm.format.vertex;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.score.SendableRiskScores;

@Value.Immutable
@JsonSerialize(as = VariableVertex.class)
@JsonDeserialize(as = VariableVertex.class)
public abstract class AbstractVariableVertex implements Vertex<UserGroup, SendableRiskScores> {

  private static final Logger log = LoggerFactory.getLogger(AbstractVariableVertex.class);

  @Override
  public abstract UserGroup getVertexId();

  @Override
  public abstract SendableRiskScores getVertexValue();
}
