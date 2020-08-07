package sharetrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.common.TestConstants;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.identity.UserId;

class SendableRiskScoresTests {

  private static final String USER_ID_1 = TestConstants.getUserId1String();

  private static final String USER_ID_2 = TestConstants.getUserId2String();

  private static final UserGroup USER_GROUP = UserGroup.builder()
      .addUsers(UserId.of(USER_ID_1), UserId.of(USER_ID_2))
      .build();

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final RiskScore RISK_SCORE_1 = RiskScore.of(USER_ID_1, INSTANT_1, MAX_RISK_SCORE);

  private static final RiskScore RISK_SCORE_2 = RiskScore.of(USER_ID_2, INSTANT_1, MAX_RISK_SCORE);

  private static final Collection<RiskScore> RISK_SCORES = ImmutableSortedSet
      .of(RISK_SCORE_1, RISK_SCORE_2);

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private SendableRiskScores sendableRiskScores;

  @BeforeEach
  final void beforeEach() {
    sendableRiskScores = SendableRiskScores.of(USER_GROUP.getUsers(), RISK_SCORES);
  }

  @Test
  final void deserialization_verifyDeserialization_returnsSendableRiskScoresWithSameValues()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(sendableRiskScores);
    AbstractSendableRiskScores deserialized = OBJECT_MAPPER
        .readValue(serialized, SendableRiskScores.class);
    assertEquals(sendableRiskScores, deserialized,
        "Deserialized value should equal original value");
  }
}
