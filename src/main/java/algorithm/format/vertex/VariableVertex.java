package algorithm.format.vertex;

import algorithm.format.vertex.serialization.VariableVertexDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.text.MessageFormat;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import model.identity.UserGroup;
import model.score.SendableRiskScores;

@Log4j2
@JsonDeserialize(using = VariableVertexDeserializer.class)
public final class VariableVertex implements Vertex<UserGroup, SendableRiskScores> {

  private final UserGroup vertexId;

  private final SendableRiskScores vertexValue;

  @JsonCreator
  private VariableVertex(UserGroup vertexId, SendableRiskScores vertexValue) {
    this.vertexId = vertexId;
    this.vertexValue = vertexValue;
  }

  public static VariableVertex of(UserGroup vertexId, SendableRiskScores vertexValue) {
    return new VariableVertex(vertexId, vertexValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    Vertex<UserGroup, SendableRiskScores> otherFactorVertex = (Vertex<UserGroup, SendableRiskScores>) o;
    boolean equalId = Objects.equals(getVertexId(), otherFactorVertex.getVertexId());
    boolean equalValue = Objects.equals(getVertexValue(), otherFactorVertex.getVertexValue());
    return equalId && equalValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getVertexId(), getVertexValue());
  }

  @Override
  public UserGroup getVertexId() {
    return UserGroup.of(vertexId);
  }

  @Override
  public SendableRiskScores getVertexValue() {
    return SendableRiskScores.of(vertexValue.getSenders(), vertexValue.getRiskScores());
  }

  @Override
  public String toString() {
    String pattern = "VariableVertex'{'vertexId={0}, vertexValue={1}'}'";
    return MessageFormat.format(pattern, vertexId, vertexValue);
  }
}
