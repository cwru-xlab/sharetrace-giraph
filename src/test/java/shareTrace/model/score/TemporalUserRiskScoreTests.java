package shareTrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import model.identity.UserId;
import model.score.TemporalUserRiskScore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class TemporalUserRiskScoreTests {

  private static final UserId USER_ID_1 = UserId.of(TestConstants.getUserId1String());

  private static final UserId USER_ID_2 = UserId.of(TestConstants.getUserId2String());

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final double MIN_RISK_SCORE = TestConstants.getMinRiskScore();

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final Instant INSTANT_2 = TestConstants.getInstant2();

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static TemporalUserRiskScore earlyUser1MinRiskScore;

  private static TemporalUserRiskScore earlyUser1MaxRiskScore;

  private static TemporalUserRiskScore earlyUser2MinRiskScore;

  private static TemporalUserRiskScore lateUser1MinRiskScore;


  @BeforeAll
  static void beforeAll() {
    earlyUser1MinRiskScore = TemporalUserRiskScore.of(USER_ID_1, INSTANT_1, MIN_RISK_SCORE);
    earlyUser2MinRiskScore = TemporalUserRiskScore.of(USER_ID_2, INSTANT_1, MIN_RISK_SCORE);
    earlyUser1MaxRiskScore = TemporalUserRiskScore.of(USER_ID_1, INSTANT_1, MAX_RISK_SCORE);
    lateUser1MinRiskScore = TemporalUserRiskScore.of(USER_ID_1, INSTANT_2, MIN_RISK_SCORE);
  }

  @Test
  final void compareTo_verifyScoresWithEqualIdAndUpdateTime_higherValueComparesGreater() {
    assertEquals(1, earlyUser1MaxRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with higher value should compare greater");
  }

  @Test
  final void compareTo_verifyScoresWithEqualValueAndUpdateTime_greaterIdComparesGreater() {
    assertEquals(1, earlyUser2MinRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with greater id should compare greater");
  }

  @Test
  final void compareTo_verifyScoresWithEqualIdAndValue_laterUpdateTimeComparesGreater() {
    assertEquals(1, lateUser1MinRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with later update time should compare greater");
  }

  @Test
  final void deserialization_verifyDeserialization_returnsTemporalUserRiskScoreWithSameValues()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(earlyUser1MinRiskScore);
    TemporalUserRiskScore deserialized = OBJECT_MAPPER
        .readValue(serialized, TemporalUserRiskScore.class);
    assertEquals(earlyUser1MinRiskScore, deserialized,
        "Deserialized value should equal original value");
  }
}

