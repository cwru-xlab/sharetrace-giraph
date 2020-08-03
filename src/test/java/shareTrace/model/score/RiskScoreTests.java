package shareTrace.model.score;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import model.score.RiskScore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class RiskScoreTests {

  private static final double MIN_RISK_SCORE = TestConstants.getMinRiskScore();

  private static final double BELOW_MIN = MIN_RISK_SCORE - 1.0;

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final double ABOVE_MAX = MAX_RISK_SCORE + 1.0;

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static RiskScore minRiskScore;

  private static RiskScore maxRiskScore;

  @BeforeAll
  static void beforeAll() {
    minRiskScore = RiskScore.of(MIN_RISK_SCORE);
    maxRiskScore = RiskScore.of(MAX_RISK_SCORE);
  }

  @Test
  final void constructor_verifyLowerBound_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> RiskScore.of(BELOW_MIN));
  }

  @Test
  final void constructor_verifyUpperBound_throwsIllegalArgumentException() {
    assertThrows(IllegalArgumentException.class, () -> RiskScore.of(ABOVE_MAX));
  }

  @Test
  final void constructor_verifyWithinBounds_doesNotThrowIllegalArgumentException() {
    assertDoesNotThrow(() -> RiskScore.of(MAX_RISK_SCORE));
    assertDoesNotThrow(() -> RiskScore.of(MIN_RISK_SCORE));
  }

  @Test
  final void compareTo_higherRiskScoreComparesGreaterThanLowerRiskScores() {

    assertEquals(1, maxRiskScore.compareTo(minRiskScore),
        "Higher risk score should compare greater to lower risk score");
    assertEquals(0, maxRiskScore.compareTo(maxRiskScore),
        "Risk scores with same value should compare equally");
    assertEquals(-1, minRiskScore.compareTo(maxRiskScore),
        "Lower risk score should compare lesser to higher risk score");
  }

  @Test
  final void deserialization_verifyDeserialization_returnsRiskScoreWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(maxRiskScore);
    RiskScore deserialized = OBJECT_MAPPER.readValue(serialized, RiskScore.class);
    assertEquals(maxRiskScore.getRiskScore(), deserialized.getRiskScore(),
        "Deserialized value should equal original value");
  }
}
