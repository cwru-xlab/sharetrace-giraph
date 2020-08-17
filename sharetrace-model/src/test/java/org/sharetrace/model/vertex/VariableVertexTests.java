package org.sharetrace.model.vertex;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.time.Instant;
import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.util.TestConstants;

class VariableVertexTests {

  private final String userId1 = TestConstants.getId1();

  private final String userId2 = TestConstants.getId2();

  private final IdGroup userGroup = IdGroup.builder()
      .addIds(userId1, userId2)
      .build();

  private final Instant instant1 = TestConstants.getInstant1();

  private final double minRiskScore = TestConstants.getMinRiskScore();

  private final RiskScore riskScore = RiskScore.builder()
      .setId(userId1)
      .setUpdateTime(instant1)
      .setValue(minRiskScore)
      .build();

  private final Collection<RiskScore> riskScores = ImmutableList.of(riskScore);

  private final SendableRiskScores sendableRiskScores = SendableRiskScores.builder()
      .setSender(userGroup.getIds())
      .setMessage(riskScores)
      .build();

  private final ObjectMapper objectMapper = TestConstants.getObjectMapper();

  private Vertex<IdGroup, SendableRiskScores> variableVertex;

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
