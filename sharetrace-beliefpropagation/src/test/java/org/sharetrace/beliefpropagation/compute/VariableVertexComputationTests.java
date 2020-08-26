package org.sharetrace.beliefpropagation.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sharetrace.beliefpropagation.BPTestConstants;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;

@ExtendWith(MockitoExtension.class)
public class VariableVertexComputationTests {

  private static final String ID_1 = BPTestConstants.getId1();

  private static final String ID_2 = BPTestConstants.getId2();

  private static final Set<String> SENDER = BPTestConstants.getId1Sender();

  private static final double MIN_SCORE = BPTestConstants.getMinScore();

  private static final double MAX_SCORE = BPTestConstants.getMaxScore();

  private static final RiskScore MIN_RISK = BPTestConstants.getMinRisk();

  private static final RiskScore MAX_RISK = BPTestConstants.getMaxRisk();

  private static final SendableRiskScores MIN_SCORES = BPTestConstants.getMinScores();

  private static final SendableRiskScores MAX_SCORES = BPTestConstants.getMaxScores();

  private static VariableVertexComputation computation;

  @Mock
  private RiskScore riskScore;

  @BeforeAll
  static void setup() {
    computation = new VariableVertexComputation();
  }

  @Test
  final void getIncomingValues_verifyResult_returnsCollectionWithAllValuesFromIterable() {
    VariableVertexValue v1 = VariableVertexValue.of(MIN_SCORES);
    VariableVertexValue v2 = VariableVertexValue.of(MAX_SCORES);
    Iterable<VariableVertexValue> iterable = ImmutableSet.of(v1, v2);
    assertEquals(ImmutableSet.of(MIN_RISK, MAX_RISK), computation.getIncomingValues(iterable));
  }

  @Test
  final void combineValues_verifyCombinedSet_returnsSetWithElementsFromBothCollections() {
    Set<RiskScore> values = ImmutableSet.of(MIN_RISK);
    Set<RiskScore> otherValues = ImmutableSet.of(MAX_RISK);
    Set<RiskScore> combined = ImmutableSet.of(MIN_RISK, MAX_RISK);
    assertEquals(combined, computation.combineValues(values, otherValues));
  }

  @Test
  final void getUpdatedValue_verifyWrapped_returnsWrappedContainingNewValues() {
    Set<RiskScore> scores = ImmutableSet.of(MIN_RISK);
    SendableRiskScores updated = computation.getUpdatedValue(SENDER, scores).getValue();
    assertEquals(SENDER, updated.getSender());
    assertEquals(scores, updated.getMessage());
  }

  @Test
  final void getMaxValueDelta_verifyOperation_returnsDifferenceOfMaxValuesFromEachCollection() {
    Set<RiskScore> scores = ImmutableSet.of(MIN_RISK);
    Set<RiskScore> otherScores = ImmutableSet.of(MAX_RISK);
    double expectedDelta = Math.abs(MIN_SCORE - MAX_SCORE);
    assertEquals(expectedDelta, computation.getMaxValueDelta(scores, otherScores));
  }

  @Test
  final void wrapReceiver_verifyWrappedValue_returnsWithSameIdAsScore() {
    when(riskScore.getId()).thenReturn(ID_1);
    assertEquals(SENDER, computation.wrapReceiver(riskScore).getIdGroup().getIds());
  }

  @Test
  final void wrapMessage_verifyWrappedValue_returnsValuesWithoutValueFromReceiver() {
    RiskScore maxRiskId2 = MAX_RISK.withId(ID_2);
    Set<RiskScore> messages = ImmutableSet.of(MIN_RISK, maxRiskId2);
    Set<RiskScore> onlyFromId1 = ImmutableSet.of(MIN_RISK);
    SendableRiskScores filtered = computation.wrapMessage(SENDER, maxRiskId2, messages).getValue();
    assertEquals(SENDER, filtered.getSender());
    assertEquals(onlyFromId1, filtered.getMessage());
  }

}
