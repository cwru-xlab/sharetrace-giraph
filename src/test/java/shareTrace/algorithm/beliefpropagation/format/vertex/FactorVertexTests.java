package shareTrace.algorithm.beliefpropagation.format.vertex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.algorithm.beliefpropagation.format.vertex.FactorVertex;
import sharetrace.algorithm.beliefpropagation.format.vertex.Vertex;
import sharetrace.common.TestConstants;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.Occurrence;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.identity.UserId;

class FactorVertexTests {

  private final UserId userId1 = UserId.of(TestConstants.getUserId1String());

  private final UserId userId2 = UserId.of(TestConstants.getUserId2String());

  private final UserGroup userGroup = UserGroup.builder()
      .addUsers(userId1, userId2)
      .build();

  private final Occurrence occurrence = Occurrence.builder()
      .setTime(TestConstants.getInstant1())
      .setDuration(TestConstants.getDuration1())
      .build();

  private final Contact contact = Contact.builder()
      .setFirstUser(userId1)
      .setSecondUser(userId2)
      .addOccurrences(occurrence)
      .build();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private Vertex<UserGroup, Contact> factorVertex;

  @BeforeEach
  final void beforeEach() {
    factorVertex = FactorVertex.builder().setVertexId(userGroup).setVertexValue(contact).build();
  }

  @Test
  final void deserialization_verifyDeserialization_returnsFactorVertexWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(factorVertex);
    FactorVertex deserialized = OBJECT_MAPPER.readValue(serialized, FactorVertex.class);
    assertEquals(factorVertex, deserialized, "Deserialized value should equal original value");
  }
}
