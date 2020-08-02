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
import model.identity.UserId;

// TODO Change UserId to String

/**
 * A {@link AbstractRiskScore} with a time for when it was last updated and an associated user.
 * <p>
 * The default implementation of {@link #compareTo(TemporalUserRiskScore)} is to first compare risk
 * score update time. If the risk scores are equally comparable based on the former, the value of
 * the risk scores is then used for comparison. Finally, if the risk scores are still equally
 * comparable based on the former two criteria, the {@link #id} is used for comparison.
 *
 * @see TemporalRiskScore
 */
@Log4j2
public final class TemporalUserRiskScore implements AbstractRiskScore,
    Comparable<TemporalUserRiskScore> {

  private static final String USER_ID_LABEL = "userId";

  private UserId id;

  private TemporalRiskScore riskScore;

  private TemporalUserRiskScore(UserId userId, TemporalRiskScore temporalRiskScore) {
    Preconditions.checkNotNull(userId);
    Preconditions.checkNotNull(temporalRiskScore);
    id = UserId.of(userId.getId());
    riskScore = TemporalRiskScore
        .of(temporalRiskScore.getTimeUpdated(), temporalRiskScore.getRiskScore());
  }

  private TemporalUserRiskScore() {
  }

  @JsonCreator
  public static TemporalUserRiskScore of(UserId userId, Instant updateTime, double riskScore) {
    return new TemporalUserRiskScore(userId, TemporalRiskScore.of(updateTime, riskScore));
  }

  public static TemporalUserRiskScore fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating TemporalUserRiskScore from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    UserId userId = UserId.fromJsonNode(jsonNode.get(USER_ID_LABEL));
    TemporalRiskScore riskScore = TemporalRiskScore.fromJsonNode(jsonNode);
    return new TemporalUserRiskScore(userId, riskScore);
  }

  static TemporalUserRiskScore fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating TemporalUserRiskScore from DataInput");
    Preconditions.checkNotNull(dataInput);
    TemporalUserRiskScore riskScore = new TemporalUserRiskScore();
    riskScore.readFields(dataInput);
    return riskScore;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    id = UserId.fromDataInput(dataInput);
    riskScore = TemporalRiskScore.fromDataInput(dataInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    id.write(dataOutput);
    riskScore.write(dataOutput);
  }

  @Override
  public int compareTo(TemporalUserRiskScore o) {
    Preconditions.checkNotNull(o);
    int compare = getUpdateTime().compareTo(o.getUpdateTime());
    if (0 == compare) {
      compare = Double.compare(getRiskScore(), o.getRiskScore());
      if (0 == compare) {
        compare = id.compareTo(o.getUserId());
      }
    }
    return compare;
  }

  @JsonGetter
  public UserId getUserId() {
    return id;
  }

  public Instant getUpdateTime() {
    return riskScore.getTimeUpdated();
  }

  @Override
  public double getRiskScore() {
    return riskScore.getRiskScore();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TemporalUserRiskScore score = (TemporalUserRiskScore) o;
    boolean equalId = Objects.equals(id, score.getUserId());
    boolean equalUpdateTime = Objects.equals(getUpdateTime(), score.getUpdateTime());
    boolean equalRiskScore = Objects.equals(getRiskScore(), score.getRiskScore());
    return equalId && equalUpdateTime && equalRiskScore;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, getRiskScore());
  }

  @Override
  public String toString() {
    return MessageFormat
        .format("TemporalUserRiskScore'{'userId={0}, updateTime={1}, riskScore={2}'}'",
            id,
            riskScore.getTimeUpdated(),
            riskScore.getRiskScore());
  }
}
