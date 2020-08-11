package sharetrace.model.score;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper type for {@link RiskScore} that is used in Hadoop.
 *
 * @see Writable
 * @see RiskScore
 */
public final class RiskScoreWritable implements Writable {

  private static final Logger LOGGER = LoggerFactory.getLogger(RiskScoreWritable.class);

  private AbstractRiskScore riskScore;

  private RiskScoreWritable() {
  }

  public static RiskScoreWritable fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    RiskScoreWritable writable = new RiskScoreWritable();
    writable.readFields(dataInput);
    return writable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeUTF(riskScore.getId());
    dataOutput.writeLong(riskScore.getUpdateTime().getEpochSecond());
    dataOutput.writeDouble(riskScore.getValue());
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    riskScore = RiskScore.builder()
        .setValue(dataInput.readDouble())
        .setUpdateTime(Instant.ofEpochSecond(dataInput.readLong()))
        .setId(dataInput.readUTF())
        .build();
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'riskScore={1}'}'", getClass().getSimpleName(), riskScore);
  }
}
