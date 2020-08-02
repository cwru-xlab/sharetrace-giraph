package algorithm.format.vertex.serialization;

import algorithm.format.vertex.VariableVertex;
import algorithm.format.vertex.Vertex;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import java.io.IOException;
import lombok.extern.log4j.Log4j2;
import model.identity.UserGroup;
import model.score.SendableRiskScores;

@Log4j2
public final class VariableVertexDeserializer extends
    StdDeserializer<Vertex<UserGroup, SendableRiskScores>> {

  private static final long serialVersionUID = 1180847517380691342L;

  private VariableVertexDeserializer() {
    super(VariableVertex.class);
  }

  @Override
  public VariableVertex deserialize(JsonParser jsonParser, DeserializationContext context)
      throws IOException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    UserGroup vertexId = UserGroup.fromJsonNode(node.get("vertexId"));
    SendableRiskScores vertexValue = SendableRiskScores.fromJsonNode(node.get("vertexValue"));
    return VariableVertex.of(vertexId, vertexValue);
  }
}
