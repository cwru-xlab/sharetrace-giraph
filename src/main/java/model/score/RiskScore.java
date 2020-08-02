package model.score;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;

/**
 * A value ranging between 0 and 1 that denotes the risk of some condition.
 */
@Log4j2
public final class RiskScore implements AbstractRiskScore, Comparable<AbstractRiskScore> {

  private static final String RISK_SCORE_RANGE_MESSAGE = "Risk score must be between 0 and 1, inclusive";

  private static final double MIN_RISK_SCORE = 0.0;

  private static final double MAX_RISK_SCORE = 1.0;

  private static final String RISK_SCORE_LABEL = "riskScore";

  private double riskScore;

  @JsonCreator
  private RiskScore(double score) {
    Preconditions.checkArgument(MIN_RISK_SCORE <= score, RISK_SCORE_RANGE_MESSAGE);
    Preconditions.checkArgument(MAX_RISK_SCORE >= score, RISK_SCORE_RANGE_MESSAGE);
    riskScore = score;
  }

  private RiskScore() {
  }

  static RiskScore fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating RiskScore from DataInput");
    RiskScore riskScore = new RiskScore();
    riskScore.readFields(dataInput);
    return riskScore;
  }

  static RiskScore fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating RiskScore from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    return of(jsonNode.get(RISK_SCORE_LABEL).asDouble());
  }

  public static RiskScore of(double riskScore) {
    return new RiskScore(riskScore);
  }

  public static String getRiskScoreLabel() {
    return RISK_SCORE_LABEL;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    riskScore = dataInput.readDouble();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeDouble(riskScore);
  }

  @Override
  public int compareTo(AbstractRiskScore o) {
    Preconditions.checkNotNull(o);
    return Double.compare(riskScore, o.getRiskScore());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RiskScore score = (RiskScore) o;
    return 0 == Double.compare(score.getRiskScore(), riskScore);
  }

  @Override
  public double getRiskScore() {
    return riskScore;
  }

  @Override
  public int hashCode() {
    return Objects.hash(riskScore);
  }

  @Override
  public String toString() {
    return MessageFormat.format("RiskScore'{'riskScore={0}'}'", riskScore);
  }
}
