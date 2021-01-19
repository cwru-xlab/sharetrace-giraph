package org.sharetrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.util.TestConstants;

class SendableRiskScoresTests {

  private static final String ID_1 = TestConstants.getId1();

  private static final String ID_2 = TestConstants.getId2();

  private static final IdGroup ID_GROUP = IdGroup.builder().addIds(ID_1, ID_2).build();

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final RiskScore RISK_SCORE_1 = RiskScore.builder()
      .id(ID_1)
      .updateTime(INSTANT_1)
      .value(MAX_RISK_SCORE)
      .build();

  private static final RiskScore RISK_SCORE_2 = RiskScore.builder()
      .id(ID_2)
      .updateTime(INSTANT_1)
      .value(MAX_RISK_SCORE)
      .build();

  private static final Collection<RiskScore> RISK_SCORES = ImmutableSortedSet
      .of(RISK_SCORE_1, RISK_SCORE_2);

  private static final ObjectMapper MAPPER = TestConstants.getObjectMapper();

  private SendableRiskScores sendableRiskScores;

  @BeforeEach
  final void beforeEach() {
    sendableRiskScores = SendableRiskScores.builder()
        .sender(ID_GROUP.getIds())
        .message(RISK_SCORES)
        .build();
  }

  @Test
  final void deserialization_verifyDeserialization_returnsSendableRiskScoresWithSameValues()
      throws JsonProcessingException {
    String serialized = MAPPER.writeValueAsString(sendableRiskScores);
    AbstractSendableRiskScores deserialized = MAPPER
        .readValue(serialized, SendableRiskScores.class);
    assertEquals(sendableRiskScores, deserialized,
        "Deserialized value should equal original value");
  }
}
