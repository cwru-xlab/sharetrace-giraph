package shareTrace.model.score;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.Collection;
import model.identity.UserGroup;
import model.identity.UserId;
import model.score.SendableRiskScores;
import model.score.TemporalUserRiskScore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class SendableRiskScoresTests {

  private static final UserId USER_ID_1 = UserId.of(TestConstants.getUserId1String());

  private static final UserId USER_ID_2 = UserId.of(TestConstants.getUserId2String());

  private static final UserGroup USER_GROUP = UserGroup.of(USER_ID_1, USER_ID_2);

  private static final double MAX_RISK_SCORE = TestConstants.getMaxRiskScore();

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final TemporalUserRiskScore RISK_SCORE_1 = TemporalUserRiskScore.of(USER_ID_1,
      INSTANT_1, MAX_RISK_SCORE);

  private static final TemporalUserRiskScore RISK_SCORE_2 = TemporalUserRiskScore.of(USER_ID_2,
      INSTANT_1, MAX_RISK_SCORE);

  private static final Collection<TemporalUserRiskScore> RISK_SCORES =
      ImmutableSortedSet.of(RISK_SCORE_1, RISK_SCORE_2);

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static SendableRiskScores sendableRiskScores;

  @BeforeAll
  static void beforeAll() {
    sendableRiskScores = SendableRiskScores.of(USER_GROUP, RISK_SCORES);
  }

  @Test
  final void deserialization_verifyDeserialization_returnsSendableRiskScoresWithSameValues()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(sendableRiskScores);
    SendableRiskScores deserialized = OBJECT_MAPPER
        .readValue(serialized, SendableRiskScores.class);
    assertEquals(sendableRiskScores, deserialized,
        "Deserialized value should equal original value");
  }
}
