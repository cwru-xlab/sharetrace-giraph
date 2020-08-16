package shareTrace.algorithm.beliefpropagation.format.vertex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sharetrace.algorithm.beliefpropagation.format.vertex.VariableVertex;
import sharetrace.algorithm.beliefpropagation.format.vertex.Vertex;
import sharetrace.common.TestConstants;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;

class VariableVertexTests {

  private final UserId userId1 = UserId.of(TestConstants.getUserId1String());

  private final UserId userId2 = UserId.of(TestConstants.getUserId2String());

  private final UserGroup userGroup = UserGroup.builder()
      .addUsers(userId1, userId2)
      .build();

  private final Instant instant1 = TestConstants.getInstant1();

  private final double minRiskScore = TestConstants.getMinRiskScore();

  private final RiskScore riskScore = RiskScore.builder()
      .setId(userId1.getId())
      .setUpdateTime(instant1)
      .setValue(minRiskScore)
      .build();

  private final Collection<RiskScore> riskScores = ImmutableList.of(riskScore);

  private final SendableRiskScores sendableRiskScores = SendableRiskScores.builder()
      .setSender(userGroup.getUsers())
      .setMessage(riskScores)
      .build();

  private final ObjectMapper objectMapper = TestConstants.getObjectMapper();

  private Vertex<UserGroup, SendableRiskScores> variableVertex;

  @BeforeEach
  final void beforeAEach() {
    variableVertex = VariableVertex.builder()
        .setVertexId(userGroup)
        .setVertexValue(sendableRiskScores)
        .build();
  }

  @Test
  final void deserialization_verifyDeserialization_returnsVariableVertexWithSameValue()
      throws JsonProcessingException {
    String serialized = objectMapper.writeValueAsString(variableVertex);
    VariableVertex deserialized = objectMapper.readValue(serialized, VariableVertex.class);
    assertEquals(variableVertex, deserialized, "Deserialized value should equal original value");
  }
}
