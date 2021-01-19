package org.sharetrace.beliefpropagation.aggregate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.io.DoubleWritable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sharetrace.beliefpropagation.BPTestConstants;

public class DeltaAggregatorTests {

  private static final Double INITIAL_VALUE = Double.POSITIVE_INFINITY;

  private static final double MAX_SCORE = BPTestConstants.getMaxScore();

  private DeltaAggregator aggregator;

  @BeforeEach
  final void setup() {
    aggregator = new DeltaAggregator();
  }

  @Test
  final void aggregate_verifyNullInput_throwsNullPointerException() {
    assertThrows(NullPointerException.class, () -> aggregator.aggregate(null));
  }

  @Test
  final void aggregate_verifyNonNullInputWithInitialValue_aggregatedValueIsUpdated() {
    aggregator.aggregate(new DoubleWritable(MAX_SCORE));
    assertEquals(MAX_SCORE, aggregator.getAggregatedValue().get());
  }

  @Test
  final void aggregate_verifyNonNullInputWithNonInitialInput_aggregatedValueIsUpdated() {
    DoubleWritable writable = new DoubleWritable(MAX_SCORE);
    aggregator.aggregate(writable);
    aggregator.aggregate(writable);
    assertEquals(MAX_SCORE + MAX_SCORE, aggregator.getAggregatedValue().get());
  }

  @Test
  final void createInitialValue_verifyInitialValueIsSet_returnsInitialValue() {
    DoubleWritable writable = aggregator.createInitialValue();
    assertEquals(INITIAL_VALUE, writable.get());
  }

  @Test
  final void reset_verifyRestValue_resetValueIsPositiveInfinity() {
    aggregator.reset();
    assertEquals(INITIAL_VALUE, aggregator.getAggregatedValue().get());
  }


}
