package shareTrace.algorithm;

import static org.junit.jupiter.api.Assertions.assertEquals;

import algorithm.format.vertex.VariableVertex;
import algorithm.format.vertex.Vertex;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import model.identity.UserGroup;
import model.identity.UserId;
import model.score.SendableRiskScores;
import model.score.TemporalUserRiskScore;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import shareTrace.TestConstants;

class VariableVertexTests {

  private static final UserId USER_ID_1 = UserId.of(TestConstants.getUserId1String());

  private static final UserId USER_ID_2 = UserId.of(TestConstants.getUserId2String());

  private static final UserGroup USER_GROUP = UserGroup.of(USER_ID_1, USER_ID_2);

  private static final Instant INSTANT_1 = TestConstants.getInstant1();

  private static final double MIN_RISK_SCORE = TestConstants.getMinRiskScore();

  private static final TemporalUserRiskScore riskScore = TemporalUserRiskScore.of(USER_ID_1,
      INSTANT_1, MIN_RISK_SCORE);

  private static final Collection<TemporalUserRiskScore> riskScores = List.of(riskScore);

  private static final SendableRiskScores sendableRiskScores = SendableRiskScores.of(USER_GROUP,
      riskScores);

  private static final ObjectMapper OBJECT_MAPPER = TestConstants.getObjectMapper();

  private static Vertex<UserGroup, SendableRiskScores> variableVertex;

  @BeforeAll
  static void beforeAll() {
    variableVertex = VariableVertex.of(USER_GROUP, sendableRiskScores);
  }

  @Test
  final void deserialization_verifyDeserialization_returnsVariableVertexWithSameValue()
      throws JsonProcessingException {
    String serialized = OBJECT_MAPPER.writeValueAsString(variableVertex);
    VariableVertex deserialized = OBJECT_MAPPER.readValue(serialized, VariableVertex.class);
    assertEquals(variableVertex, deserialized, "Deserialized value should equal original value");
  }
}
