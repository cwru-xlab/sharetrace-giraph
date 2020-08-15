package sharetrace.algorithm.beliefpropagation.combiner;

import org.apache.giraph.graph.VertexValueCombiner;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;
import sharetrace.model.score.SendableRiskScoresWritable;

public class SendableRiskScoresCombiner implements VertexValueCombiner<SendableRiskScoresWritable> {

  @Override
  public void combine(SendableRiskScoresWritable original, SendableRiskScoresWritable other) {
    SendableRiskScores originalValue = original.getSendableRiskScores();
    SendableRiskScores otherValue = other.getSendableRiskScores();
    SendableRiskScores combined;
    if (originalValue.getMessage().size() == 1) {
      combined = finalizeRecent(originalValue, otherValue);
    } else {
      combined = finalizeMerged(originalValue, otherValue);
    }
    original.setSendableRiskScores(combined);
  }

  private SendableRiskScores finalizeRecent(SendableRiskScores original, SendableRiskScores other) {
    RiskScore moreRecent = getMoreRecent(original.getMessage().first(), other.getMessage().first());
    return SendableRiskScores.builder()
        .addSender(UserId.of(moreRecent.getId()))
        .addMessage(moreRecent)
        .build();
  }

  private RiskScore getMoreRecent(RiskScore score, RiskScore otherScore) {
    return score.getUpdateTime().isAfter(otherScore.getUpdateTime()) ? score : otherScore;
  }

  private SendableRiskScores finalizeMerged(SendableRiskScores original, SendableRiskScores other) {
    return SendableRiskScores.builder()
        .addAllMessage(original.getMessage())
        .addAllMessage(other.getMessage())
        .addAllSender(original.getSender())
        .build();
  }
}
