package org.sharetrace.beliefpropagation.param;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.sharetrace.beliefpropagation.compute.MasterComputer;

public class BPContext {

  private static final double DEFAULT_RISK_SCORE = 0.0;

  private static final double TRANSMISSION_PROBABILITY = 0.7;

  private static final long CUTOFF_VALUE = 14L;

  private static final ChronoUnit CUTOFF_TIME_UNIT = ChronoUnit.DAYS;

  private static final Instant OCCURRENCE_LOOKBACK_CUTOFF = MasterComputer.getInitializedAt()
      .minus(CUTOFF_VALUE, CUTOFF_TIME_UNIT);

  public static double getDefaultRiskScore() {
    return DEFAULT_RISK_SCORE;
  }

  public static double getTransmissionProbability() {
    return TRANSMISSION_PROBABILITY;
  }

  public static Instant getOccurrenceLookbackCutoff() {
    return OCCURRENCE_LOOKBACK_CUTOFF;
  }
}
