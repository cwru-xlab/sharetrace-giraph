package org.sharetrace.beliefpropagation.combine;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.hadoop.io.Writable;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.beliefpropagation.param.BPContext;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A combiner that is used for both factor and variable vertex types.
 * <p>
 * For factor vertices, {@link Occurrence}s from both {@link Contact}s are combined, keeping the ids
 * of the original vertex. Additionally, expired occurrences are removed.
 * <p>
 * For variable vertices, the {@link SendableRiskScores} that has the most recently updated {@link
 * RiskScore} is used.
 */
public class FactorGraphVertexValueCombiner implements VertexValueCombiner<FactorGraphWritable> {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FactorGraphVertexValueCombiner.class);

  private static final Instant CUTOFF = BPContext.getOccurrenceLookbackCutoff();

  @Override
  public void combine(FactorGraphWritable original, FactorGraphWritable other) {
    Preconditions.checkNotNull(original);
    Preconditions.checkNotNull(other);
    VertexType originalType = original.getType();
    VertexType otherType = other.getType();
    if (originalType.equals(otherType)) {
      Writable combined;
      if (originalType.equals(VertexType.FACTOR)) {
        Contact originalValue = ((FactorVertexValue) original.getWrapped()).getValue();
        Contact otherValue = ((FactorVertexValue) other.getWrapped()).getValue();
        combined = combine(originalValue, otherValue);
      } else {
        SendableRiskScores originalValue = ((VariableVertexValue) original.getWrapped()).getValue();
        SendableRiskScores otherValue = ((VariableVertexValue) other.getWrapped()).getValue();
        combined = combine(originalValue, otherValue);
      }
      original.setWrapped(combined);
    }
  }

  private Writable combine(Contact original, Contact other) {
    Preconditions.checkNotNull(original);
    Preconditions.checkNotNull(other);
    Contact combined = Contact.builder()
        .firstUser(original.getFirstUser())
        .secondUser(original.getSecondUser())
        .addAllOccurrences(original.getOccurrences())
        .addAllOccurrences(other.getOccurrences())
        .build();
    return removedExpiredValues(combined);
  }

  // Remove occurrences from the Contact if they occurred before the expiration date.
  @VisibleForTesting
  FactorVertexValue removedExpiredValues(Contact value) {
    Set<Occurrence> withoutExpiredValues = value.getOccurrences().stream()
        .filter(occurrence -> occurrence.getTime().isAfter(getCutoff()))
        .collect(Collectors.toSet());
    return FactorVertexValue.of(Contact.copyOf(value).withOccurrences(withoutExpiredValues));
  }

  @VisibleForTesting
  Instant getCutoff() {
    return CUTOFF;
  }

  private Writable combine(SendableRiskScores original, SendableRiskScores other) {
    Preconditions.checkNotNull(original);
    Preconditions.checkNotNull(other);
    return VariableVertexValue.of(getMoreRecent(original, other));
  }

  private SendableRiskScores getMoreRecent(SendableRiskScores scores,
      SendableRiskScores otherScores) {
    Instant latestScoreTime = scores.getMessage().last().getUpdateTime();
    Instant latestOtherScoreTime = otherScores.getMessage().last().getUpdateTime();
    return latestScoreTime.isAfter(latestOtherScoreTime) ? scores : otherScores;
  }
}
