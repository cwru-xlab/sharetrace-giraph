package org.sharetrace.model.score;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.SortedSet;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@JsonSerialize(as = SendableRiskScores.class)
@JsonDeserialize(as = SendableRiskScores.class)
public abstract class AbstractSendableRiskScores implements
    Sendable<SortedSet<String>, SortedSet<RiskScore>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSendableRiskScores.class);

  @Override
  @Value.NaturalOrder
  public abstract SortedSet<String> getSender();

  @Override
  @Value.NaturalOrder
  public abstract SortedSet<RiskScore> getMessage();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkState(!getSender().isEmpty());
    getSender().forEach(s -> Preconditions.checkArgument(!Strings.isNullOrEmpty(s)));
  }
}
