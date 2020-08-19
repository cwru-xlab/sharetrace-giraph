package org.sharetrace.model.identity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.util.TestConstants;

class IdGroupTests {

  private static final String ID_1 = TestConstants.getId1();

  private static final String ID_2 = TestConstants.getId2();

  private static final ObjectMapper MAPPER = TestConstants.getObjectMapper();

  private IdGroup ids;

  @BeforeEach
  final void beforeEach() {
    ids = IdGroup.builder().addIds(ID_1, ID_2).build();
  }

  @Test
  final void serialization_verifySerialization_doesNotThrowException() {
    assertDoesNotThrow(() -> MAPPER.writeValueAsString(ids));
  }

  @Test
  final void deserialization_verifyDeserialization_returnsIdGroupWithSameValue()
      throws JsonProcessingException {
    String serialized = MAPPER.writeValueAsString(ids);
    IdGroup deserialized = MAPPER.readValue(serialized, IdGroup.class);
    assertEquals(ids, deserialized, "Deserialized value should equal original value");
  }
}
