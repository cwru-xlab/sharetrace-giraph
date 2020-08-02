package model.score;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;

/**
 * A {@link AbstractRiskScore} with a time for when it was last updated.
 * <p>
 * The default implementation of {@link #compareTo(TemporalRiskScore)} is to first compare {@link
 * #riskScore}. If the risk scores are equally comparable based on the former, then {@link
 * #timeUpdated} is then used for comparison.
 *
 * @see AbstractRiskScore
 */
@Log4j2
public final class TemporalRiskScore implements AbstractRiskScore, Comparable<TemporalRiskScore> {

  private static final String UPDATE_TIME_LABEL = "updateTime";

  private static final String EPOCH_TIME_LABEL = "epochSecond";

  private Instant timeUpdated;

  private RiskScore riskScore;

  @JsonCreator
  private TemporalRiskScore(Instant updateTime, double score) {
    Preconditions.checkNotNull(updateTime);
    timeUpdated = updateTime;
    riskScore = RiskScore.of(score);
  }

  private TemporalRiskScore() {
  }

  public static TemporalRiskScore of(Instant updateTime, double riskScore) {
    return new TemporalRiskScore(updateTime, riskScore);
  }

  static TemporalRiskScore fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating TemporalRiskScore from DataInput");
    Preconditions.checkNotNull(dataInput);
    TemporalRiskScore temporalRiskScore = new TemporalRiskScore();
    temporalRiskScore.readFields(dataInput);
    return temporalRiskScore;
  }

  static TemporalRiskScore fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating TemporalRiskScore from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    long time = jsonNode.get(UPDATE_TIME_LABEL).asLong();
    Instant updateTime = Instant.ofEpochSecond(time);
    RiskScore riskScore = RiskScore.fromJsonNode(jsonNode);
    return new TemporalRiskScore(updateTime, riskScore.getRiskScore());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    timeUpdated = Instant.ofEpochSecond(dataInput.readLong());
    riskScore = RiskScore.fromDataInput(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeLong(timeUpdated.getEpochSecond());
    riskScore.write(dataOutput);
  }

  @Override
  public int compareTo(TemporalRiskScore o) {
    Preconditions.checkNotNull(o);
    int compare = timeUpdated.compareTo(o.getTimeUpdated());
    if (0 == compare) {
      compare = Double.compare(riskScore.getRiskScore(), o.getRiskScore());
    }
    return compare;
  }

  @Override
  public double getRiskScore() {
    return riskScore.getRiskScore();
  }

  @JsonGetter(UPDATE_TIME_LABEL)
  public Instant getTimeUpdated() {
    return timeUpdated;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TemporalRiskScore score = (TemporalRiskScore) o;
    boolean equalUpdateTime = Objects.equals(timeUpdated, score.getTimeUpdated());
    boolean equalRiskScore = Objects.equals(riskScore.getRiskScore(), score.getRiskScore());
    return equalUpdateTime && equalRiskScore;
  }

  @Override
  public int hashCode() {
    return Objects.hash(timeUpdated, getRiskScore());
  }

  @Override
  public String toString() {
    return MessageFormat.format("TemporalRiskScore'{'updateTime={0}, riskScore={1}'}'",
        timeUpdated,
        riskScore);
  }
}
