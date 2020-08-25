package org.sharetrace.beliefpropagation.combine;

import com.google.common.base.Preconditions;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  // Logging messages
  private static final String CHECKING_IF_SAME_MSG =
      "Combiner checking if vertex types are the same...";
  private static final String PROCEEDING_MSG =
      "Combiner proceeding since vertex types are the same";
  private static final String COMBINING_FACTORS_MSG = "Combining factor vertices...";
  private static final String COMBINING_VARIABLES_MSG = "Combining variable vertices...";
  private static final String SUCCESSFULLY_COMBINED_MSG = "Successfully combined vertices";
  private static final String STOPPED_EXECUTION_MSG =
      "Combiner stopped execution since vertex types are different";
  private static final String NULL_CONTACT_MSG = "Null contact cannot be combined";
  private static final String NULL_SCORE_MSG = "Null risk scores cannot be combined";

  private static final Logger LOGGER =
      LoggerFactory.getLogger(FactorGraphVertexValueCombiner.class);

  @Override
  public void combine(FactorGraphWritable original, FactorGraphWritable other) {
    VertexType originalType = original.getType();
    VertexType otherType = other.getType();
    LOGGER.debug(CHECKING_IF_SAME_MSG);
    if (originalType.equals(otherType)) {
      LOGGER.debug(PROCEEDING_MSG);
      Writable combined;
      if (originalType.equals(VertexType.FACTOR)) {
        LOGGER.debug(COMBINING_FACTORS_MSG);
        Contact originalValue = ((FactorVertexValue) original.getWrapped()).getValue();
        Contact otherValue = ((FactorVertexValue) other.getWrapped()).getValue();
        combined = combine(originalValue, otherValue);
      } else {
        LOGGER.debug(COMBINING_VARIABLES_MSG);
        SendableRiskScores originalValue =
            ((VariableVertexValue) original.getWrapped()).getValue();
        SendableRiskScores otherValue =
            ((VariableVertexValue) other.getWrapped()).getValue();
        combined = combine(originalValue, otherValue);
      }
      LOGGER.debug(SUCCESSFULLY_COMBINED_MSG);
      original.setWrapped(combined);
    }
    LOGGER.debug(STOPPED_EXECUTION_MSG);
  }

  private Writable combine(Contact original, Contact other) {
    Preconditions.checkNotNull(original, NULL_CONTACT_MSG);
    Preconditions.checkNotNull(other, NULL_CONTACT_MSG);
    return FactorVertexValue.of(Contact.builder()
        .firstUser(original.getFirstUser())
        .secondUser(original.getSecondUser())
        .addAllOccurrences(original.getOccurrences())
        .addAllOccurrences(other.getOccurrences())
        .build());
  }

  private Writable combine(SendableRiskScores original, SendableRiskScores other) {
    Preconditions.checkNotNull(original, NULL_SCORE_MSG);
    Preconditions.checkNotNull(other, NULL_SCORE_MSG);
    return VariableVertexValue.of(getMoreRecent(original, other));
  }

  private SendableRiskScores getMoreRecent(SendableRiskScores scores,
      SendableRiskScores otherScores) {
    Instant latestScoreTime = scores.getMessage().last().getUpdateTime();
    Instant latestOtherScoreTime = otherScores.getMessage().last().getUpdateTime();
    return latestScoreTime.isAfter(latestOtherScoreTime) ? scores : otherScores;
  }
}
