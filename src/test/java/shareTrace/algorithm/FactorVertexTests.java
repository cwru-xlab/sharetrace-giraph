package shareTrace.algorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import algorithm.format.vertex.FactorVertex;
import algorithm.format.vertex.Vertex;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import model.contact.Contact;
import model.contact.TemporalOccurrence;
import model.identity.UserGroup;
import model.identity.UserId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

@Log4j2
class FactorVertexTests {

  private static final UserId USER_ID_1 = UserId.of(TestConstants.getUserId1String());

  private static final UserId USER_ID_2 = UserId.of(TestConstants.getUserId2String());

  private static final UserGroup USER_GROUP = UserGroup.of(USER_ID_1, USER_ID_2);

  private static final TemporalOccurrence OCCURRENCE =
      TemporalOccurrence.of(TestConstants.getInstant1(), TestConstants.getDuration1());

  private static final Contact CONTACT = Contact.of(USER_ID_1, USER_ID_2, OCCURRENCE);

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static Vertex<UserGroup, Contact> factorVertex;

  @BeforeAll
  static void beforeAll() {
    factorVertex = FactorVertex.of(USER_GROUP, CONTACT);
  }

  @Test
  final void deserialization_verifyDeserialization_returnsFactorVertexWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(factorVertex);
    FactorVertex deserialized = OBJECT_MAPPER.readValue(serialized, FactorVertex.class);
    assertEquals(factorVertex, deserialized, "Deserialized value should equal original value");
  }
}
