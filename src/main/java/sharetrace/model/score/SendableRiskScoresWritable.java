package sharetrace.model.score;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.TreeSet;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.identity.AbstractUserId;
import sharetrace.model.identity.UserId;

/**
 * Wrapper type for {@link SendableRiskScores} that is used in Hadoop.
 *
 * @see Writable
 * @see SendableRiskScores
 */
public final class SendableRiskScoresWritable implements Writable {

  private static final Logger log = LoggerFactory.getLogger(SendableRiskScoresWritable.class);

  private AbstractSendableRiskScores sendableRiskScores;

  private SendableRiskScoresWritable() {
  }

  private SendableRiskScoresWritable(AbstractSendableRiskScores sendableRiskScores) {
    Preconditions.checkNotNull(sendableRiskScores);
    this.sendableRiskScores = sendableRiskScores;
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
      dataOutput.writeLong(riskScore.getUpdateTime().getEpochSecond());
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
  }

  public SendableRiskScores getSendableRiskScores() {
    return SendableRiskScores.copyOf(sendableRiskScores);
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'sendableRiskScores={1}'}'",
        getClass().getSimpleName(), sendableRiskScores);
  }
}
