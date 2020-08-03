package algorithm.format.vertex.serialization;

import algorithm.format.vertex.VariableVertex;
import algorithm.format.vertex.Vertex;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import model.identity.UserGroup;
import model.score.SendableRiskScores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VariableVertexDeserializer extends
    StdDeserializer<Vertex<UserGroup, SendableRiskScores>> {

  private static final Logger log = LoggerFactory.getLogger(VariableVertexDeserializer.class);

  private static final long serialVersionUID = 1180847517380691342L;

  private VariableVertexDeserializer() {
    super(VariableVertex.class);
  }

  @Override
  public VariableVertex deserialize(JsonParser jsonParser, DeserializationContext context)
      throws IOException {
    Preconditions.checkNotNull(jsonParser);
    Preconditions.checkNotNull(context);
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    UserGroup vertexId = UserGroup.fromJsonNode(node.get("vertexId"));
    SendableRiskScores vertexValue = SendableRiskScores.fromJsonNode(node.get("vertexValue"));
    return VariableVertex.of(vertexId, vertexValue);
  }
}
