package sharetrace.algorithm.beliefpropagation.format.vertex;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.score.SendableRiskScores;

@JsonSerialize(as = VariableVertex.class)
@JsonDeserialize(as = VariableVertex.class)
@Value.Immutable
public abstract class AbstractVariableVertex implements FactorGraphVertex<UserGroup, SendableRiskScores> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractVariableVertex.class);

  @Value.Default
  @Override
  public VertexType getType() {
    return VertexType.VARIABLE;
  }

  @Override
  public abstract UserGroup getVertexId();

  @Override
  public abstract SendableRiskScores getVertexValue();
}
