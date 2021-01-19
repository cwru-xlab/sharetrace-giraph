package org.sharetrace.beliefpropagation;

import com.google.common.collect.ImmutableSortedSet;
import java.time.Duration;
import java.time.Instant;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;

public final class BPTestConstants {

  private static final Instant MIN_INSTANT = Instant.MIN;

  private static final Instant MAX_INSTANT = Instant.MAX;

  private static final Instant ZERO_INSTANT = Instant.EPOCH;

  private static final Duration ZERO_DURATION = Duration.ZERO;

  private static final String ID_1 = "ID_1";

  private static final String ID_2 = "ID_2";

  private static final double MIN_SCORE = 0.0;

  private static final double MAX_SCORE = 1.0;

  private static final ImmutableSortedSet<String> ID_1_SENDER = ImmutableSortedSet.of(ID_1);

  private static final RiskScore MIN_RISK =
      RiskScore.builder().id(ID_1).updateTime(MIN_INSTANT).value(MIN_SCORE).build();

  private static final RiskScore MAX_RISK =
      RiskScore.builder().id(ID_1).updateTime(MAX_INSTANT).value(MAX_SCORE).build();

  private static final Occurrence MIN_OCCURRENCE =
      Occurrence.builder().time(MIN_INSTANT).duration(ZERO_DURATION).build();

  private static final Occurrence MAX_OCCURRENCE =
      Occurrence.builder().time(MAX_INSTANT).duration(ZERO_DURATION).build();

  private static final SendableRiskScores MIN_SCORES =
      SendableRiskScores.builder().addSender(ID_1).addMessage(MIN_RISK).build();

  private static final SendableRiskScores MAX_SCORES =
      SendableRiskScores.builder().addSender(ID_1).addMessage(MAX_RISK).build();

  public static Instant getMinInstant() {
    return MIN_INSTANT;
  }

  public static Instant getMaxInstant() {
    return MAX_INSTANT;
  }

  public static Instant getZeroInstant() {
    return ZERO_INSTANT;
  }

  public static Duration getZeroDuration() {
    return ZERO_DURATION;
  }

  public static String getId1() {
    return ID_1;
  }

  public static String getId2() {
    return ID_2;
  }

  public static double getMinScore() {
    return MIN_SCORE;
  }

  public static double getMaxScore() {
    return MAX_SCORE;
  }

  public static ImmutableSortedSet<String> getId1Sender() {
    return ID_1_SENDER;
  }

  public static RiskScore getMinRisk() {
    return MIN_RISK;
  }

  public static RiskScore getMaxRisk() {
    return MAX_RISK;
  }

  public static Occurrence getMinOccurrence() {
    return MIN_OCCURRENCE;
  }

  public static Occurrence getMaxOccurrence() {
    return MAX_OCCURRENCE;
  }

  public static SendableRiskScores getMinScores() {
    return MIN_SCORES;
  }

  public static SendableRiskScores getMaxScores() {
    return MAX_SCORES;
  }
}
