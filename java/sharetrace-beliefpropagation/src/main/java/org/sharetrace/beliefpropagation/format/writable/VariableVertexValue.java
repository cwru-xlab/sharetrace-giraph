package org.sharetrace.beliefpropagation.format.writable;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.Writable;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper type for {@link SendableRiskScores} that is used in Hadoop.
 *
 * @see Writable
 * @see SendableRiskScores
 */
public final class VariableVertexValue implements Writable {

  private static final Logger LOGGER = LoggerFactory.getLogger(VariableVertexValue.class);

  private static final String TO_STRING_PATTERN = "{0}'{'sendableRiskScores={1}'}'";

  private SendableRiskScores sendableRiskScores;

  private VariableVertexValue() {
  }

  private VariableVertexValue(SendableRiskScores sendableRiskScores) {
    Preconditions.checkNotNull(sendableRiskScores);
    this.sendableRiskScores = SendableRiskScores.copyOf(sendableRiskScores);
  }

  public static VariableVertexValue of(SendableRiskScores sendableRiskScores) {
    return new VariableVertexValue(sendableRiskScores);
  }

  public static VariableVertexValue fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    VariableVertexValue writable = new VariableVertexValue();
    writable.readFields(dataInput);
    return writable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(sendableRiskScores.getSender().size());
    for (String userId : sendableRiskScores.getSender()) {
      dataOutput.writeUTF(userId);
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
    SortedSet<String> userIds = new TreeSet<>();
    int nUserIds = dataInput.readInt();
    for (int iUserId = 0; iUserId < nUserIds; iUserId++) {
      userIds.add(dataInput.readUTF());
    }
    SortedSet<RiskScore> riskScores = new TreeSet<>();
    int nRiskScores = dataInput.readInt();
    for (int iRiskScore = 0; iRiskScore < nRiskScores; iRiskScore++) {
      RiskScore riskScore = RiskScore.builder()
          .id(dataInput.readUTF())
          .updateTime(Instant.ofEpochMilli(dataInput.readLong()))
          .value(dataInput.readDouble())
          .build();
      riskScores.add(riskScore);
    }
    sendableRiskScores = SendableRiskScores.builder()
        .sender(userIds)
        .message(riskScores)
        .build();
  }

  public SendableRiskScores getValue() {
    return SendableRiskScores.copyOf(sendableRiskScores);
  }

  @Override
  public String toString() {
    return MessageFormat.format(TO_STRING_PATTERN, getClass().getSimpleName(), sendableRiskScores);
  }
}
