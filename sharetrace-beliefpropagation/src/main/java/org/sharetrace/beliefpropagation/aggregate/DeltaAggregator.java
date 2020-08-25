package org.sharetrace.beliefpropagation.aggregate;

import com.google.common.base.Preconditions;
import java.text.MessageFormat;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeltaAggregator implements Aggregator<DoubleWritable> {

  // Logging messages
  private static final String CREATING_MSG =
      "Creating initial value for VertexValueDeltaAggregator";
  private static final String RESETTING_MSG = "Resetting aggregated value...";
  private static final String SETTING_PATTERN = "Setting aggregated value from {0} to {1}";
  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaAggregator.class);
  private static final String NULL_WRITABLE_MSG = "Aggregated writable must not be null";

  private static final String TO_STRING_PATTERN = "{0}'{'aggregatedValue={1}'}'";

  private static final Double INITIAL_VALUE = Double.POSITIVE_INFINITY;

  private DoubleWritable aggregatedValue = new DoubleWritable(INITIAL_VALUE);

  @Override
  public void aggregate(DoubleWritable a) {
    Preconditions.checkNotNull(a, NULL_WRITABLE_MSG);
    Double prev = aggregatedValue.get();
    double val = a.get();
    double newVal;
    if (prev.equals(Double.POSITIVE_INFINITY)) {
      newVal = val;
    } else {
      newVal = prev + a.get();
      LOGGER.debug(MessageFormat.format(SETTING_PATTERN, prev, val));
    }
    setAggregatedValue(new DoubleWritable(newVal));
  }

  @Override
  public DoubleWritable createInitialValue() {
    LOGGER.debug(CREATING_MSG);
    return new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public DoubleWritable getAggregatedValue() {
    return new DoubleWritable(aggregatedValue.get());
  }

  @Override
  public void setAggregatedValue(DoubleWritable a) {
    Preconditions.checkNotNull(a, NULL_WRITABLE_MSG);
    aggregatedValue.set(a.get());
  }

  @Override
  public void reset() {
    LOGGER.debug(RESETTING_MSG);
    aggregatedValue = new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public String toString() {
    return MessageFormat.format(TO_STRING_PATTERN, getClass().getSimpleName(), aggregatedValue);
  }
}
