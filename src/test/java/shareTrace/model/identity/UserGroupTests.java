package shareTrace.model.identity;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.identity.UserGroup;
import model.identity.UserId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class UserGroupTests {

    private static final String USER_ID_1 = TestConstants.getUserId1String();

    private static final String USER_ID_2 = TestConstants.getUserId2String();

    private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

    private static UserGroup users;

    @BeforeAll
    static void beforeAll() {
        users = UserGroup.of(UserId.of(USER_ID_1), UserId.of(USER_ID_2));
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
