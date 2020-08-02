package shareTrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import lombok.extern.log4j.Log4j2;
import model.score.TemporalRiskScore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

@Log4j2
class TemporalRiskScoreTests {

  private static final Instant TEST_INSTANT_1 = TestConstants.getTestInstant1();

  private static final Instant TEST_INSTANT_2 = TestConstants.getTestInstant2();

  private static final double MIN_RISK_SCORE = TestConstants.getMinRiskScore();

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static TemporalRiskScore minRiskScoreEarly;

  private static TemporalRiskScore minRiskScoreLate;

  private static TemporalRiskScore maxRiskScoreLate;

  @BeforeAll
  static void beforeAll() {
    minRiskScoreEarly = TemporalRiskScore.of(TEST_INSTANT_1, MIN_RISK_SCORE);
    minRiskScoreLate = TemporalRiskScore.of(TEST_INSTANT_1, MAX_RISK_SCORE);
    maxRiskScoreLate = TemporalRiskScore.of(TEST_INSTANT_2, MAX_RISK_SCORE);
  }

  @Test
  final void compareTo_verifyRiskScoreCompareWithEqualUpdateTime_maxRiskScoreComparesGreater() {
    assertEquals(1, maxRiskScoreLate.compareTo(minRiskScoreLate),
        "High risk score should compare greater to lower risk score");
  }

  @Test
  final void compareTo_verifyRiskScoreCompareWithEqualRiskScore_laterUpdateTimeComparesGreater() {
    assertEquals(1, minRiskScoreLate.compareTo(minRiskScoreEarly),
        "Risk score with later update time should compare greater to risk score with earlier update time");
  }

  @Test
  final void compareTo_verifyRiskScoresWithEqualValueAndUpdateTime_returnsZero() {
    assertEquals(0, minRiskScoreLate.compareTo(minRiskScoreLate),
        "Risk scores with equal value and update time should compare equally");
  }

  @Test
  final void deserialization_verifyDeserialization_returnsTemporalUserRiskWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(minRiskScoreEarly);
    TemporalRiskScore deserialized = OBJECT_MAPPER.readValue(serialized, TemporalRiskScore.class);
    assertEquals(minRiskScoreEarly, deserialized, "Deserialized value should equal original value");
  }
}
