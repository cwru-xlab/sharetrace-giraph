package algorithm.format.vertex.serialization;

import algorithm.format.vertex.FactorVertex;
import algorithm.format.vertex.Vertex;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.google.common.base.Preconditions;
import java.io.IOException;
import model.contact.Contact;
import model.identity.UserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FactorVertexDeserializer extends StdDeserializer<Vertex<UserGroup, Contact>> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexDeserializer.class);

  private static final long serialVersionUID = -1287311490439021139L;

  private FactorVertexDeserializer() {
    super(FactorVertex.class);
  }

  public static FactorVertexDeserializer newInstance() {
    return new FactorVertexDeserializer();
  }

  @Override
  public Vertex<UserGroup, Contact> deserialize(JsonParser jsonParser,
      DeserializationContext context)
      throws IOException {
    Preconditions.checkNotNull(jsonParser);
    Preconditions.checkNotNull(context);
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    UserGroup vertexId = UserGroup.fromJsonNode(node.get("vertexId"));
    Contact vertexValue = Contact.fromJsonNode(node.get("vertexValue"));
    return FactorVertex.of(vertexId, vertexValue);
  }
}
