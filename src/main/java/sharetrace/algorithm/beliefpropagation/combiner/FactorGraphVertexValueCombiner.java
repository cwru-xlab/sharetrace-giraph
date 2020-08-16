package sharetrace.algorithm.beliefpropagation.combiner;

import java.time.Instant;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.hadoop.io.Writable;
import sharetrace.algorithm.beliefpropagation.format.vertex.VertexType;
import sharetrace.algorithm.beliefpropagation.format.writable.ContactWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.FactorGraphWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.SendableRiskScoresWritable;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.Occurrence;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;

/**
 * A combiner that is used for both factor and variable vertex types.
 * <p>
 * For factor vertices, {@link Occurrence}s from both {@link Contact}s are combined, keeping the
 * {@link UserId}s of the original vertex.
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
        Contact originalValue = ((ContactWritable) original.getWrapped()).getContact();
        Contact otherValue = ((ContactWritable) other.getWrapped()).getContact();
        combined = combine(originalValue, otherValue);
      } else {
        SendableRiskScores originalValue =
            ((SendableRiskScoresWritable) original.getWrapped()).getSendableRiskScores();
        SendableRiskScores otherValue =
            ((SendableRiskScoresWritable) other.getWrapped()).getSendableRiskScores();
        combined = combine(originalValue, otherValue);
      }
      original.setWrapped(combined);
    }
  }

  private Writable combine(Contact original, Contact other) {
    return Contact.builder()
        .setFirstUser(original.getFirstUser())
        .setSecondUser(original.getSecondUser())
        .addAllOccurrences(original.getOccurrences())
        .addAllOccurrences(other.getOccurrences())
        .build()
        .wrap();
  }

  private Writable combine(SendableRiskScores original, SendableRiskScores other) {
    SendableRiskScores moreRecent = getMoreRecent(original, other);
    return SendableRiskScores.copyOf(moreRecent).wrap();
  }

  private SendableRiskScores getMoreRecent(SendableRiskScores scores,
      SendableRiskScores otherScores) {
    Instant latestScoreTime = scores.getMessage().last().getUpdateTime();
    Instant latestOtherScoreTime = otherScores.getMessage().last().getUpdateTime();
    return latestScoreTime.isAfter(latestOtherScoreTime) ? scores : otherScores;
  }
}
