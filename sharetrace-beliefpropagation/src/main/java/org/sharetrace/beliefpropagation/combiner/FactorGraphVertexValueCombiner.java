package org.sharetrace.beliefpropagation.combiner;

import java.time.Instant;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.hadoop.io.Writable;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.vertex.VertexType;

/**
 * A combiner that is used for both factor and variable vertex types.
 * <p>
 * For factor vertices, {@link Occurrence}s from both {@link Contact}s are combined, keeping the ids
 * of the original vertex.
 * <p>
 * For variable vertices, the {@link SendableRiskScores} that has the most recently updated {@link
 * RiskScore} is used.
 */
public class FactorGraphVertexValueCombiner implements VertexValueCombiner<FactorGraphWritable> {

  @Override
  public void combine(FactorGraphWritable original, FactorGraphWritable other) {
    VertexType originalType = original.getType();
    VertexType otherType = other.getType();
    if (originalType.equals(otherType)) {
      Writable combined;
      if (originalType.equals(VertexType.FACTOR)) {
        Contact originalValue = ((FactorVertexValue) original.getWrapped()).getContact();
        Contact otherValue = ((FactorVertexValue) other.getWrapped()).getContact();
        combined = combine(originalValue, otherValue);
      } else {
        SendableRiskScores originalValue =
            ((VariableVertexValue) original.getWrapped()).getSendableRiskScores();
        SendableRiskScores otherValue =
            ((VariableVertexValue) other.getWrapped()).getSendableRiskScores();
        combined = combine(originalValue, otherValue);
      }
      original.setWrapped(combined);
    }
  }

  private Writable combine(Contact original, Contact other) {
    return FactorVertexValue.of(Contact.builder()
        .setFirstUser(original.getFirstUser())
        .setSecondUser(original.getSecondUser())
        .addAllOccurrences(original.getOccurrences())
        .addAllOccurrences(other.getOccurrences())
        .build());
  }

  private Writable combine(SendableRiskScores original, SendableRiskScores other) {
    return VariableVertexValue.of(getMoreRecent(original, other));
  }

  private SendableRiskScores getMoreRecent(SendableRiskScores scores,
      SendableRiskScores otherScores) {
    Instant latestScoreTime = scores.getMessage().last().getUpdateTime();
    Instant latestOtherScoreTime = otherScores.getMessage().last().getUpdateTime();
    return latestScoreTime.isAfter(latestOtherScoreTime) ? scores : otherScores;
  }
}
