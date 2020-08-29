package org.sharetrace.model.vertex;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.score.SendableRiskScores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@JsonSerialize(as = VariableVertex.class)
@JsonDeserialize(as = VariableVertex.class)
public abstract class AbstractVariableVertex implements FactorGraphVertex<IdGroup, SendableRiskScores> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractVariableVertex.class);

  @Override
  @Value.Derived
  public VertexType getType() {
    return VertexType.VARIABLE;
  }

  @Override
  public abstract IdGroup getVertexId();

  @Override
  public abstract SendableRiskScores getVertexValue();
}
