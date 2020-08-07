package sharetrace.model.score;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.common.Wrappable;
import sharetrace.model.identity.UserId;

@Value.Immutable
@JsonSerialize(as = SendableRiskScores.class)
@JsonDeserialize(as = SendableRiskScores.class)
public abstract class AbstractSendableRiskScores implements
    Sendable<SortedSet<UserId>, SortedSet<RiskScore>>,
    Wrappable<SendableRiskScoresWritable> {

  private static final Logger log = LoggerFactory.getLogger(AbstractSendableRiskScores.class);

  @Override
  @Value.NaturalOrder
  public abstract SortedSet<UserId> getSender();

  @Override
  @Value.NaturalOrder
  public abstract SortedSet<RiskScore> getMessage();

  @Override
  public final SendableRiskScoresWritable wrap() {
    return SendableRiskScoresWritable.of(this);
  }

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkState(!getMessage().isEmpty());
  }
}
