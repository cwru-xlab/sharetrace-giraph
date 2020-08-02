package shareTrace.model.contact;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import model.contact.TemporalOccurrence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class TemporalOccurrenceTests {

    private static final Instant TEST_INSTANT_1 = TestConstants.getInstant1();

    private static final Instant TEST_INSTANT_2 = TestConstants.getInstant2();

    private static final Duration TEST_DURATION_1 = TestConstants.getDuration1();

    private static final Duration TEST_DURATION_2 = TestConstants.getDuration2();

    private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

    private static TemporalOccurrence occurrence1;

    private static TemporalOccurrence occurrence2;

    @BeforeAll
    static void beforeAll() {
        occurrence1 = TemporalOccurrence.of(TEST_INSTANT_1, TEST_DURATION_1);
        occurrence2 = TemporalOccurrence.of(TEST_INSTANT_2, TEST_DURATION_2);
    }

    @Test
    final void compareTo_verifyEqualOccurrences_returnZero() {
        assertEquals(0,
            occurrence1.compareTo(occurrence1),
            "Temporal occurrences with same time and duration should compare equally");
    }

    @Test
    final void compareTo_verifyUnequalOccurrences_returnOneWhenComparedToEarlierOccurrence() {
        assertEquals(1,
            occurrence2.compareTo(occurrence1),
            "Temporal occurrences that occur later should compare to be greater than earlier occurrences");
    }

    @Test
    final void compareTo_verifySemiEqualOccurrences_returnOneWhenComparedToOccurrenceWithSameTimeButLongerDuration() {
        TemporalOccurrence occurrence2SameTime = TemporalOccurrence
            .of(TEST_INSTANT_1, TEST_DURATION_2);
        assertEquals(1,
            occurrence2SameTime.compareTo(occurrence1),
            "Temporal occurrences that occur at the "
                + "same time should then compare by duration: longer "
                + "compares greater than shorter");
    }

    @Test
    final void serialization_verifySerialization_doesNotThrowException() {
        assertDoesNotThrow(() -> OBJECT_MAPPER.writeValueAsString(occurrence1));
    }

    @Test
    final void deserialization_verifyDeserialization_returnsUserIdWithSameValue()
        throws JsonProcessingException {
        String serialized = OBJECT_MAPPER.writeValueAsString(occurrence1);
        TemporalOccurrence deserialized = OBJECT_MAPPER
            .readValue(serialized, TemporalOccurrence.class);
        assertEquals(occurrence1, deserialized, "Deserialized value should equal original value");
    }
}
