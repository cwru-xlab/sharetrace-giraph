package org.sharetrace.model.contact;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.util.TestConstants;

class ContactTests {

  private static final Instant TEST_INSTANT_1 = TestConstants.getInstant1();

  private static final Instant TEST_INSTANT_2 = TestConstants.getInstant2();

  private static final Duration TEST_DURATION_1 = TestConstants.getDuration1();

  private static final Duration TEST_DURATION_2 = TestConstants.getDuration2();

  private static final String ID_1 = TestConstants.getId1();

  private static final String ID_2 = TestConstants.getId2();

  private static final ObjectMapper MAPPER = TestConstants.getObjectMapper();

  private String userId1;

  private String userId2;

  private Occurrence occurrence1;

  private Occurrence occurrence2;

  private AbstractContact contact;

  @BeforeEach
  final void beforeEach() {
    userId1 = ID_1;
    userId2 = ID_2;
    occurrence1 = Occurrence.builder().time(TEST_INSTANT_1).duration(TEST_DURATION_1).build();
    occurrence2 = Occurrence.builder().time(TEST_INSTANT_2).duration(TEST_DURATION_2).build();
    contact = Contact.builder()
        .firstUser(userId1)
        .secondUser(userId2)
        .addOccurrences(occurrence1, occurrence2)
        .build();
  }

  @Test
  final void constructor_withSameUserAsBothUsers_throwsIllegalStateException() {
    assertThrows(IllegalStateException.class,
        () -> Contact.builder()
            .firstUser(userId1)
            .secondUser(userId1)
            .addOccurrences(occurrence1)
            .build());
  }

  @Test
  final void equals_verifyDifferentUserOrderings_firstSecondEqualsSecondFirst() {
    AbstractContact swappedUsers = Contact.builder()
        .firstUser(userId2)
        .secondUser(userId1)
        .addOccurrences(occurrence1, occurrence2)
        .build();
    assertEquals(contact, swappedUsers, "Contacts with the same users should be equal");
  }

  @Test
  final void serialization_verifySerialization_doesNotThrowException() {
    assertDoesNotThrow(() -> MAPPER.writeValueAsString(contact));
  }

  @Test
  final void deserialization_verifyDeserialization_returnsUserIdWithSameValue()
      throws JsonProcessingException {
    String serialized = MAPPER.writeValueAsString(contact);
    AbstractContact deserialized = MAPPER.readValue(serialized, Contact.class);
    assertEquals(contact, deserialized, "Deserialized value should equal original value");
  }
}
