package org.sharetrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.util.TestConstants;

class RiskScoreTests {

  private static final String ID_1 = TestConstants.getId1();

  private static final String ID_2 = TestConstants.getId2();

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final double MIN_RISK_SCORE = TestConstants.getMinRiskScore();

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final Instant INSTANT_2 = TestConstants.getInstant2();

  private static final ObjectMapper MAPPER = TestConstants.getObjectMapper();

  private RiskScore earlyUser1MinRiskScore;

  private RiskScore earlyUser1MaxRiskScore;

  private RiskScore earlyUser2MinRiskScore;

  private RiskScore lateUser1MinRiskScore;

  @BeforeEach
  final void beforeEach() {
    earlyUser1MinRiskScore = RiskScore.builder()
        .setId(ID_1)
        .setUpdateTime(INSTANT_1)
        .setValue(MIN_RISK_SCORE)
        .build();
    earlyUser2MinRiskScore = RiskScore.builder()
        .setId(ID_2)
        .setUpdateTime(INSTANT_1)
        .setValue(MIN_RISK_SCORE)
        .build();
    earlyUser1MaxRiskScore = RiskScore.builder()
        .setId(ID_1)
        .setUpdateTime(INSTANT_1)
        .setValue(MAX_RISK_SCORE)
        .build();
    lateUser1MinRiskScore = RiskScore.builder()
        .setId(ID_1)
        .setUpdateTime(INSTANT_2)
        .setValue(MIN_RISK_SCORE)
        .build();
  }

  @Test
  final void compareTo_verifyScoresWithEqualIdAndUpdateTime_higherValueComparesGreater() {
    Assertions.assertEquals(1, earlyUser1MaxRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with higher value should compare greater");
  }

  @Test
  final void compareTo_verifyScoresWithEqualValueAndUpdateTime_greaterIdComparesGreater() {
    Assertions.assertEquals(1, earlyUser2MinRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with greater id should compare greater");
  }

  @Test
  final void compareTo_verifyScoresWithEqualIdAndValue_laterUpdateTimeComparesGreater() {
    Assertions.assertEquals(1, lateUser1MinRiskScore.compareTo(earlyUser1MinRiskScore),
        "Score with later update time should compare greater");
  }

  @Test
  final void deserialization_verifyDeserialization_returnsTemporalUserRiskScoreWithSameValues()
      throws JsonProcessingException {
    String serialized = MAPPER.writeValueAsString(earlyUser1MinRiskScore);
    AbstractRiskScore deserialized = MAPPER.readValue(serialized, RiskScore.class);
    assertEquals(earlyUser1MinRiskScore, deserialized,
        "Deserialized value should equal original value");
  }
}

