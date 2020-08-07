package sharetrace.model.identity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.common.TestConstants;

class UserIdTests {

  private static final String USER_ID_1 = TestConstants.getUserId1String();

  private static final String USER_ID_2 = TestConstants.getUserId2String();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private Identifiable<String> userId1;

  private Identifiable<String> userId2;

  @BeforeEach
  final void beforeEach() {
    userId1 = UserId.of(USER_ID_1);
    userId2 = UserId.of(USER_ID_2);
  }

  @Test
  final void constructor_verifyGetId_returnsSameIdUsedToInstantiate() {
    assertEquals(USER_ID_1, userId1.getId(), "UserId should be equal to value from getter");
  }

  @Test
  final void equals_verifySameTwoUsers_ReturnUserIdsAreEqual() {
    assertEquals(userId1, userId1, "UserIds should be equal if they have the same value");
  }

  @Test
  final void equals_verifyTwoDifferentUser_ReturnUserIdsAreNotEqual() {
    assertNotEquals(userId1, userId2, "UserIds should not be equal if they have different values");
  }

  @Test
  final void serialization_verifySerialization_doesNotThrowException() {
    assertDoesNotThrow(() -> OBJECT_MAPPER.writeValueAsString(userId1));
  }

  @Test
  final void deserialization_verifyDeserialization_returnsUserIdWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(userId1);
    UserId deserialized = OBJECT_MAPPER.readValue(serialized, UserId.class);
    assertEquals(USER_ID_1, deserialized.getId(), "Deserialized value should equal original value");
  }
}
