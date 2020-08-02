package shareTrace.model.contact;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import model.contact.Contact;
import model.contact.TemporalOccurrence;
import model.identity.UserId;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class ContactTests {

    private static final Instant TEST_INSTANT_1 = TestConstants.getInstant1();

    private static final Instant TEST_INSTANT_2 = TestConstants.getInstant2();

    private static final Duration TEST_DURATION_1 = TestConstants.getDuration1();

    private static final Duration TEST_DURATION_2 = TestConstants.getDuration2();

    private static final String USER_ID_1_STRING = TestConstants.getUserId1String();

    private static final String USER_ID_2_STRING = TestConstants.getUserId2String();

    private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

    private static UserId userId1;

    private static UserId userId2;

    private static TemporalOccurrence occurrence1;

    private static TemporalOccurrence occurrence2;

    private static Contact contact;

    @BeforeAll
    static void beforeAll() {
        userId1 = UserId.of(USER_ID_1_STRING);
        userId2 = UserId.of(USER_ID_2_STRING);
        occurrence1 = TemporalOccurrence.of(TEST_INSTANT_1, TEST_DURATION_1);
        occurrence2 = TemporalOccurrence.of(TEST_INSTANT_2, TEST_DURATION_2);
        contact = Contact.of(userId1, userId2, occurrence1, occurrence2);
    }

    @Test
    final void constructor_withSameUserAsBothUsers_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class,
            () -> Contact.of(userId1, userId1, occurrence1));
    }

    @Test
    final void constructor_withNoOccurrences_throwsIllegalArgumentException() {
        assertThrows(IllegalArgumentException.class, () -> Contact.of(userId1, userId2));
    }

    @Test
    final void equals_verifyDifferentUserOrderings_firstSecondEqualsSecondFirst() {
        Contact swappedUsers = Contact
            .of(contact.getSecondUser(), contact.getFirstUser(), contact.getOccurrences());
        assertEquals(contact, swappedUsers, "Contacts with the same users should be equal");
    }

    @Test
    final void serialization_verifySerialization_doesNotThrowException() {
        assertDoesNotThrow(() -> OBJECT_MAPPER.writeValueAsString(contact));
    }

    @Test
    final void deserialization_verifyDeserialization_returnsUserIdWithSameValue()
        throws JsonProcessingException {
        String serialized = OBJECT_MAPPER.writeValueAsString(contact);
        Contact deserialized = OBJECT_MAPPER.readValue(serialized, Contact.class);
        assertEquals(contact, deserialized, "Deserialized value should equal original value");
    }
}
