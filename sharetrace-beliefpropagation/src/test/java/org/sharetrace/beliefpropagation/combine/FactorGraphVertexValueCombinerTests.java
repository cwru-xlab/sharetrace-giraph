package org.sharetrace.beliefpropagation.combine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSortedSet;
import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.sharetrace.beliefpropagation.BPTestConstants;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.vertex.VertexType;

@ExtendWith(MockitoExtension.class)
public class FactorGraphVertexValueCombinerTests {

  private static final String ID_1 = BPTestConstants.getId1();

  private static final String ID_2 = BPTestConstants.getId2();

  private static final Instant MIN_INSTANT = BPTestConstants.getMinInstant();

  private static final Instant MAX_INSTANT = BPTestConstants.getMaxInstant();

  private static final Instant ZERO_INSTANT = BPTestConstants.getZeroInstant();

  private static final Occurrence MIN_OCCURRENCE = BPTestConstants.getMinOccurrence();

  private static final Occurrence MAX_OCCURRENCE = BPTestConstants.getMaxOccurrence();

  private static final Duration DURATION = BPTestConstants.getZeroDuration();

  private static final double MIN_SCORE = BPTestConstants.getMinScore();

  @Mock
  private Contact contact;

  @Mock
  private Contact otherContact;

  @Mock
  private SendableRiskScores scores;

  @Mock
  private SendableRiskScores otherScores;

  @Mock
  private FactorGraphWritable writableWithContact;

  @Mock
  private FactorGraphWritable writableWithOtherContact;

  @Mock
  private FactorGraphWritable writableWithScores;

  @Mock
  private FactorGraphWritable writableWithOtherScores;

  private Occurrence occurrence;

  private Occurrence otherOccurrence;

  private RiskScore riskScore;

  private RiskScore otherRiskScore;

  private FactorGraphVertexValueCombiner combiner;

  @BeforeEach
  final void setup() {
    combiner = spy(new FactorGraphVertexValueCombiner());
  }

  @Test
  final void combine_verifyVertexValueCheck_returnsUnmodifiedWritable() {
    writableWithContact = spy(FactorGraphWritable.ofFactorVertex(FactorVertexValue.of(contact)));
    writableWithOtherContact =
        spy(FactorGraphWritable.ofFactorVertex(FactorVertexValue.of(otherContact)));
    when(writableWithContact.getType()).thenReturn(VertexType.FACTOR);
    when(writableWithScores.getType()).thenReturn(VertexType.VARIABLE);
    combiner.combine(writableWithContact, writableWithScores);
    verify(writableWithContact, never()).getWrapped();
    verify(writableWithScores, never()).getWrapped();
  }

  @Test
  final void combine_verifyCombiningFactorVertexValues_returnsWritableWithCombinedOccurrence() {
    when(contact.getFirstUser()).thenReturn(ID_1);
    when(contact.getSecondUser()).thenReturn(ID_2);
    occurrence = Occurrence.builder().time(MIN_INSTANT).duration(DURATION).build();
    when(contact.getOccurrences()).thenReturn(ImmutableSortedSet.of(occurrence));
    otherOccurrence = Occurrence.builder().time(MAX_INSTANT).duration(DURATION).build();
    when(otherContact.getOccurrences()).thenReturn(ImmutableSortedSet.of(otherOccurrence));
    SortedSet<Occurrence> occurrences = ImmutableSortedSet.of(otherOccurrence);
    writableWithContact = spy(FactorGraphWritable.ofFactorVertex(FactorVertexValue.of(contact)));
    writableWithOtherContact =
        spy(FactorGraphWritable.ofFactorVertex(FactorVertexValue.of(otherContact)));
    combiner.combine(writableWithContact, writableWithOtherContact);
    Contact combined = ((FactorVertexValue) writableWithContact.getWrapped()).getValue();
    assertEquals(occurrences, combined.getOccurrences());
  }

  @Test
  final void removedExpiredValues_verifyRemovalOfExpiredValues_returnsValueWithoutExpiredOccurrences() {
    Contact contact = Contact.builder()
        .firstUser(ID_1)
        .secondUser(ID_2)
        .addOccurrences(MIN_OCCURRENCE, MAX_OCCURRENCE)
        .build();
    Contact filteredContact = Contact.copyOf(contact).withOccurrences(MAX_OCCURRENCE);
    when(combiner.getCutoff()).thenReturn(ZERO_INSTANT);
    assertEquals(filteredContact, combiner.removedExpiredValues(contact).getValue());
  }

  @Test
  final void combine_verifyCombiningVariableVertexValues_returnsWritableWithMoreRecentScores() {
    riskScore = RiskScore.builder().id(ID_1).updateTime(MIN_INSTANT).value(MIN_SCORE).build();
    otherRiskScore = RiskScore.builder().id(ID_2).updateTime(MAX_INSTANT).value(MIN_SCORE).build();
    when(scores.getMessage()).thenReturn(ImmutableSortedSet.of(riskScore));
    when(otherScores.getMessage()).thenReturn(ImmutableSortedSet.of(otherRiskScore));
    writableWithScores = spy(FactorGraphWritable.ofVariableVertex(VariableVertexValue.of(scores)));
    writableWithOtherScores =
        spy(FactorGraphWritable.ofVariableVertex(VariableVertexValue.of(otherScores)));
    combiner.combine(writableWithScores, writableWithOtherScores);
    VariableVertexValue combined = (VariableVertexValue) writableWithScores.getWrapped();
    assertEquals(otherScores, combined.getValue());
  }
}
