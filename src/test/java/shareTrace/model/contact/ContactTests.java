package sharetrace.model.contact;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.common.TestConstants;
import sharetrace.model.identity.UserId;

class ContactTests {

  private static final Instant TEST_INSTANT_1 = TestConstants.getInstant1();

  private static final Instant TEST_INSTANT_2 = TestConstants.getInstant2();

  private static final Duration TEST_DURATION_1 = TestConstants.getDuration1();

  private static final Duration TEST_DURATION_2 = TestConstants.getDuration2();

  private static final String USER_ID_1_STRING = TestConstants.getUserId1String();

  private static final String USER_ID_2_STRING = TestConstants.getUserId2String();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private UserId userId1;

  private UserId userId2;

  private Occurrence occurrence1;

  private Occurrence occurrence2;

  private AbstractContact contact;

  @BeforeEach
  final void beforeEach() {
    userId1 = UserId.of(USER_ID_1_STRING);
    userId2 = UserId.of(USER_ID_2_STRING);
    occurrence1 = Occurrence.builder().setTime(TEST_INSTANT_1).setDuration(TEST_DURATION_1).build();
    occurrence2 = Occurrence.builder().setTime(TEST_INSTANT_2).setDuration(TEST_DURATION_2).build();
    contact = Contact.builder()
        .setFirstUser(userId1)
        .setSecondUser(userId2)
        .addOccurrences(occurrence1, occurrence2)
        .build();
  }

  @Test
  final void constructor_withSameUserAsBothUsers_throwsIllegalStateException() {
    assertThrows(IllegalStateException.class,
        () -> Contact.builder()
            .setFirstUser(userId1)
            .setSecondUser(userId1)
            .addOccurrences(occurrence1)
            .build());
  }

  @Test
  final void constructor_withNoOccurrences_throwsIllegalStateException() {
    assertThrows(IllegalStateException.class,
        () -> Contact.builder()
            .setFirstUser(userId1)
            .setSecondUser(userId2)
            .build());
  }

  @Test
  final void equals_verifyDifferentUserOrderings_firstSecondEqualsSecondFirst() {
    AbstractContact swappedUsers = Contact.builder()
        .setFirstUser(userId2)
        .setSecondUser(userId1)
        .addOccurrences(occurrence1, occurrence2)
        .build();
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
    AbstractContact deserialized = OBJECT_MAPPER.readValue(serialized, Contact.class);
    assertEquals(contact, deserialized, "Deserialized value should equal original value");
  }
}
