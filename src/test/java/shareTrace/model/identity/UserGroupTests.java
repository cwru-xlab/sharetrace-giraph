package sharetrace.model.identity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.common.TestConstants;

class UserGroupTests {

  private static final String USER_ID_1 = TestConstants.getUserId1String();

  private static final String USER_ID_2 = TestConstants.getUserId2String();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private UserGroup users;

  @BeforeEach
  final void beforeEach() {
    users = UserGroup.builder().addUsers(UserId.of(USER_ID_1), UserId.of(USER_ID_2)).build();
  }

  @Test
  final void serialization_verifySerialization_doesNotThrowException() {
    assertDoesNotThrow(() -> OBJECT_MAPPER.writeValueAsString(users));
  }

  @Test
  final void deserialization_verifyDeserialization_returnsUserIdWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(users);
    UserGroup deserialized = OBJECT_MAPPER.readValue(serialized, UserGroup.class);
    assertEquals(users, deserialized, "Deserialized value should equal original value");
  }
}
