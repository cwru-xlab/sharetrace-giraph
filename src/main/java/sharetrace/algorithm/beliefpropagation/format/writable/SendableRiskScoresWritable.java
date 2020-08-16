package sharetrace.algorithm.beliefpropagation.format.writable;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.TreeSet;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.format.vertex.VertexType;
import sharetrace.model.common.Wrappable;
import sharetrace.model.identity.AbstractUserId;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.AbstractRiskScore;
import sharetrace.model.score.AbstractSendableRiskScores;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;

/**
 * Wrapper type for {@link SendableRiskScores} that is used in Hadoop.
 *
 * @see Writable
 * @see SendableRiskScores
 */
public final class SendableRiskScoresWritable implements Writable, Wrappable<FactorGraphWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SendableRiskScoresWritable.class);

  private static final Comparator<? super RiskScore> COMPARE_BY_RISK_SCORE =
      Comparator.comparing(RiskScore::getValue);

  private AbstractSendableRiskScores sendableRiskScores;

  private AbstractRiskScore maxRiskScore;

  private SendableRiskScoresWritable() {
  }

  private SendableRiskScoresWritable(AbstractSendableRiskScores sendableRiskScores) {
    Preconditions.checkNotNull(sendableRiskScores);
    this.sendableRiskScores = sendableRiskScores;
    this.maxRiskScore = Collections.max(sendableRiskScores.getMessage(), COMPARE_BY_RISK_SCORE);
  }

  public static SendableRiskScoresWritable of(AbstractSendableRiskScores sendableRiskScores) {
    return new SendableRiskScoresWritable(sendableRiskScores);
  }

  public static SendableRiskScoresWritable fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    SendableRiskScoresWritable writable = new SendableRiskScoresWritable();
    writable.readFields(dataInput);
    return writable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(sendableRiskScores.getSender().size());
    for (AbstractUserId userId : sendableRiskScores.getSender()) {
      dataOutput.writeUTF(userId.getId());
    }
    dataOutput.writeInt(sendableRiskScores.getMessage().size());
    for (RiskScore riskScore : sendableRiskScores.getMessage()) {
      dataOutput.writeUTF(riskScore.getId());
      dataOutput.writeLong(riskScore.getUpdateTime().toEpochMilli());
      dataOutput.writeDouble(riskScore.getValue());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Collection<UserId> userIds = new TreeSet<>();
    int nUserIds = dataInput.readInt();
    for (int iUserId = 0; iUserId < nUserIds; iUserId++) {
      userIds.add(UserId.of(dataInput.readUTF()));
    }
    Collection<RiskScore> riskScores = new TreeSet<>();
    int nRiskScores = dataInput.readInt();
    for (int iRiskScore = 0; iRiskScore < nRiskScores; iRiskScore++) {
      RiskScore riskScore = RiskScore.builder()
          .setId(dataInput.readUTF())
          .setUpdateTime(Instant.ofEpochSecond(dataInput.readLong()))
          .setValue(dataInput.readDouble())
          .build();
      riskScores.add(riskScore);
    }
    sendableRiskScores = SendableRiskScores.builder()
        .setSender(userIds)
        .setMessage(riskScores)
        .build();
    maxRiskScore = Collections.max(sendableRiskScores.getMessage(), COMPARE_BY_RISK_SCORE);
  }

  public SendableRiskScores getSendableRiskScores() {
    return SendableRiskScores.copyOf(sendableRiskScores);
  }

  public void setSendableRiskScores(SendableRiskScores riskScores) {
    sendableRiskScores = SendableRiskScores.copyOf(riskScores);
    maxRiskScore = Collections.max(riskScores.getMessage());
  }

  public RiskScore getMaxRiskScore() {
    return RiskScore.copyOf(maxRiskScore);
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'sendableRiskScores={1}, maxRiskScore={2}'}'",
        getClass().getSimpleName(), sendableRiskScores, maxRiskScore);
  }

  @Override
  public FactorGraphWritable wrap() {
    return FactorGraphWritable.of(VertexType.VARIABLE, this);
  }
}
