package model.score;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.extern.log4j.Log4j2;
import model.identity.UserGroup;
import model.identity.UserId;
import org.apache.hadoop.io.Writable;

@Log4j2
public final class SendableRiskScores implements Writable {

  private static final String SENDER_LABEL = "senders";

  private static final String RISK_SCORES_LABEL = "riskScores";

  private UserGroup users;

  private SortedSet<TemporalUserRiskScore> scores;

  @JsonCreator
  private SendableRiskScores(Collection<UserId> senders,
      Collection<TemporalUserRiskScore> riskScores) {
    Preconditions.checkNotNull(senders);
    Preconditions.checkNotNull(riskScores);
    users = UserGroup.of(senders);
    scores = ImmutableSortedSet.copyOf(riskScores);
  }

  private SendableRiskScores() {
  }

  public static SendableRiskScores fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating SendableRiskScores from DataInput");
    Preconditions.checkNotNull(dataInput);
    SendableRiskScores sendableRiskScores = new SendableRiskScores();
    sendableRiskScores.readFields(dataInput);
    return sendableRiskScores;
  }

  public static SendableRiskScores fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating SendableRiskScores from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    Iterator<JsonNode> riskScoresIterator = jsonNode.get(RISK_SCORES_LABEL).elements();
    SortedSet<TemporalUserRiskScore> riskScores = new TreeSet<>();
    while (riskScoresIterator.hasNext()) {
      riskScores.add(TemporalUserRiskScore.fromJsonNode(riskScoresIterator.next()));
    }
    UserGroup senders = UserGroup.fromJsonNode(jsonNode.get(SENDER_LABEL));
    return new SendableRiskScores(senders, riskScores);
  }

  public static SendableRiskScores of(UserId sender, Collection<TemporalUserRiskScore> riskScores) {
    return new SendableRiskScores(UserGroup.of(sender), riskScores);
  }

  public static SendableRiskScores of(Collection<UserId> senders,
      Collection<TemporalUserRiskScore> riskScores) {
    return new SendableRiskScores(senders, riskScores);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    SortedSet<UserId> users = new TreeSet<>();
    int nUsers = dataInput.readInt();
    for (int iUser = 0; iUser < nUsers; iUser++) {
      users.add(UserId.fromDataInput(dataInput));
    }
    this.users = UserGroup.of(users);
    SortedSet<TemporalUserRiskScore> scores = new TreeSet<>();
    int nScores = dataInput.readInt();
    for (int iScore = 0; iScore < nScores; iScore++) {
      scores.add(TemporalUserRiskScore.fromDataInput(dataInput));
    }
    this.scores = ImmutableSortedSet.copyOf(scores);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    users.write(dataOutput);
    dataOutput.write(scores.size());
    for (TemporalUserRiskScore riskScore : scores) {
      riskScore.write(dataOutput);
    }
  }

  public SendableRiskScores withRiskScores(Collection<TemporalUserRiskScore> riskScores) {
    Preconditions.checkNotNull(riskScores);
    if (!scores.equals(riskScores)) {
      return new SendableRiskScores(users, riskScores);
    }
    return this;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    SendableRiskScores scores = (SendableRiskScores) o;
    boolean equalSenders = Objects.equals(users, scores.getSenders());
    boolean equalRiskScores = Objects.equals(this.scores, scores.getRiskScores());
    return equalSenders && equalRiskScores;
  }

  @JsonGetter
  public UserGroup getSenders() {
    return UserGroup.of(users);
  }

  public SortedSet<TemporalUserRiskScore> getRiskScores() {
    return ImmutableSortedSet.copyOf(scores);
  }

  @Override
  public int hashCode() {
    return Objects.hash(users, scores);
  }

  @Override
  public String toString() {
    return MessageFormat
        .format("SendableRiskScores'{'senders={0}, riskScores={1}'}'", users, scores);
  }
}
