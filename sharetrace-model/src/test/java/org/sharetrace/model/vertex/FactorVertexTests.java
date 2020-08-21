package org.sharetrace.model.vertex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.util.TestConstants;

class FactorVertexTests {

  private final String userId1 = TestConstants.getId1();

  private final String userId2 = TestConstants.getId2();

  private final IdGroup userGroup = IdGroup.builder()
      .addIds(userId1, userId2)
      .build();

  private final Occurrence occurrence = Occurrence.builder()
      .time(TestConstants.getInstant1())
      .duration(TestConstants.getDuration1())
      .build();

  private final Contact contact = Contact.builder()
      .firstUser(userId1)
      .secondUser(userId2)
      .addOccurrences(occurrence)
      .build();

  private static final ObjectMapper MAPPER = TestConstants.getObjectMapper();

  private Vertex<IdGroup, Contact> factorVertex;

  @BeforeEach
  final void beforeEach() {
    factorVertex = FactorVertex.builder().vertexId(userGroup).vertexValue(contact).build();
  }

  @Test
  final void deserialization_verifyDeserialization_returnsFactorVertexWithSameValue()
      throws JsonProcessingException {
    String serialized = MAPPER.writeValueAsString(factorVertex);
    FactorVertex deserialized = MAPPER.readValue(serialized, FactorVertex.class);
    assertEquals(factorVertex, deserialized, "Deserialized value should equal original value");
  }
}
