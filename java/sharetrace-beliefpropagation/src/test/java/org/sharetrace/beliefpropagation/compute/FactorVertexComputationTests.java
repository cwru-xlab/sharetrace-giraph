package org.sharetrace.beliefpropagation.compute;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sharetrace.beliefpropagation.BPTestConstants;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;

@ExtendWith(MockitoExtension.class)
public class FactorVertexComputationTests {

  private static final Instant MIN_INSTANT = BPTestConstants.getMinInstant();

  private static final Instant MAX_INSTANT = BPTestConstants.getMaxInstant();

  private static final String ID_1 = BPTestConstants.getId1();

  private static final double MIN_SCORE = BPTestConstants.getMinScore();

  private static final double MAX_SCORE = BPTestConstants.getMaxScore();

  private static final ImmutableSortedSet<String> ID_1_SENDER = BPTestConstants.getId1Sender();

  private static final RiskScore MIN_RISK = BPTestConstants.getMinRisk();

  private static final RiskScore MAX_RISK = BPTestConstants.getMaxRisk();

  private static final Occurrence MIN_OCCURRENCE = BPTestConstants.getMinOccurrence();

  private static final Occurrence MAX_OCCURRENCE = BPTestConstants.getMaxOccurrence();

  private static final SendableRiskScores MIN_SCORES = BPTestConstants.getMinScores();

  private static final SendableRiskScores MAX_SCORES = BPTestConstants.getMaxScores();

  @Mock
  private SendableRiskScores scores;

  @Mock
  private Contact contact;

  private static FactorVertexComputation computation;

  @BeforeAll
  static void setup() {
    computation = spy(new FactorVertexComputation());
  }

  @Test
  final void retainValidMessages_verifyContactWithNoOccurrences_returnsEmptySet() {
    when(contact.getOccurrences()).thenReturn(ImmutableSortedSet.of());
    Set<SendableRiskScores> emptySet = ImmutableSortedSet.of();
    assertEquals(emptySet, computation.retainValidMessages(contact, emptySet));
  }

  @Test
  final void getIncomingValues_verifyEmptyIterable_returnsEmptySet() {
    assertEquals(ImmutableSet.of(), computation.getIncomingValues(ImmutableSet.of()));
  }

  @Test
  final void getIncomingValues_verifyNonEmptyIterable_returnsSetContainingAllVertexValueScores() {
    VariableVertexValue v1 = VariableVertexValue.of(MIN_SCORES);
    VariableVertexValue v2 = VariableVertexValue.of(MAX_SCORES);
    Iterable<VariableVertexValue> iterable = ImmutableSet.of(v1, v2);
    assertEquals(ImmutableSet.of(MIN_SCORES, MAX_SCORES), computation.getIncomingValues(iterable));
  }

  @Test
  final void retainValidMessages_verifyContactWithOccurrences_retainsScoresUpdatedBeforeLatest() {
    ImmutableSortedSet<Occurrence> occurrences = ImmutableSortedSet
        .of(MIN_OCCURRENCE, MAX_OCCURRENCE);
    when(contact.getOccurrences()).thenReturn(occurrences);
    Set<SendableRiskScores> multipleScores = ImmutableSet.of(MIN_SCORES, MAX_SCORES);
    Set<SendableRiskScores> retained = computation.retainValidMessages(contact, multipleScores);
    assertEquals(MIN_SCORES, ImmutableList.copyOf(retained).get(0));
  }

  @Test
  final void retainIfUpdatedBefore_verifyRetainedScores_returnsOnlyScoresBeforeCutoff() {
    SendableRiskScores scores = SendableRiskScores.builder()
        .addMessage(MIN_RISK, MAX_RISK)
        .addSender(ID_1)
        .build();
    SendableRiskScores retained = computation.retainIfUpdatedBefore(scores, MAX_INSTANT);
    ImmutableSortedSet<RiskScore> message = retained.getMessage();
    assertEquals(1, message.size());
    assertEquals(MIN_RISK, message.first());
  }

  @Test
  final void wrapReceiver_verifyScoresWrapped_returnsFactorGraphVertexIdWithSameIdAsScores() {
    when(scores.getSender()).thenReturn(ID_1_SENDER);
    assertEquals(ID_1_SENDER, computation.wrapReceiver(scores).getIdGroup().getIds());
  }

  @Test
  final void wrapMessage_verifyNonTransmitted_returnsValueWithDefaultRiskScore() {
    when(computation.getInitializedAt()).thenReturn(MIN_INSTANT);
    when(computation.isTransmitted()).thenReturn(false);
    when(scores.getMessage()).thenReturn(ImmutableSortedSet.of());
    VariableVertexValue value = computation.wrapMessage(ID_1_SENDER, scores);
    RiskScore valueScore = value.getValue().getMessage().first();
    assertEquals(MIN_SCORE, valueScore.getValue());
    assertEquals(ID_1, valueScore.getId());
    assertEquals(MIN_INSTANT, valueScore.getUpdateTime());
  }

  @Test
  final void wrapMessage_verifyNonEmptyMessage_returnsValueWithMaxRiskScoreValue() {
    when(computation.isTransmitted()).thenReturn(true);
    when(scores.getMessage()).thenReturn(ImmutableSortedSet.of(MIN_RISK, MAX_RISK));
    VariableVertexValue value = computation.wrapMessage(ID_1_SENDER, scores);
    RiskScore valueScore = value.getValue().getMessage().first();
    assertEquals(MAX_SCORE, valueScore.getValue());
    assertEquals(ID_1, valueScore.getId());
    assertEquals(MAX_INSTANT, valueScore.getUpdateTime());
  }

}
