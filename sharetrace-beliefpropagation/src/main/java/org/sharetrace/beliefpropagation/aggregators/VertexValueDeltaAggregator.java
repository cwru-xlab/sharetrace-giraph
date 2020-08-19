package org.sharetrace.beliefpropagation.aggregators;

import com.google.common.base.Preconditions;
import java.text.MessageFormat;
import java.util.Optional;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VertexValueDeltaAggregator implements Aggregator<DoubleWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VertexValueDeltaAggregator.class);

  private static final Double INITIAL_VALUE = Double.POSITIVE_INFINITY;

  private DoubleWritable aggregatedValue = new DoubleWritable(INITIAL_VALUE);

  @Override
  public void aggregate(DoubleWritable a) {
    Preconditions.checkNotNull(a, "Aggregated writable must not be null");
    double prev = aggregatedValue.get();
    double curr = prev + a.get();
    LOGGER.debug("Setting aggregated value from " + prev + " to " + curr);
    setAggregatedValue(new DoubleWritable(curr));
  }

  @Override
  public DoubleWritable createInitialValue() {
    LOGGER.debug("Creating initial value for VertexValueDeltaAggregator");
    return new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public DoubleWritable getAggregatedValue() {
    return new DoubleWritable(aggregatedValue.get());
  }

  @Override
  public void setAggregatedValue(DoubleWritable a) {
    Preconditions.checkNotNull(a, "Value of aggregated value cannot be null");
    aggregatedValue.set(a.get());
  }

  @Override
  public void reset() {
    LOGGER.debug("Resetting aggregated value...");
    if (Optional.ofNullable(aggregatedValue).isPresent()) {
      aggregatedValue.set(INITIAL_VALUE);
    } else {
      aggregatedValue = new DoubleWritable(INITIAL_VALUE);
    }
  }

  @Override
  public String toString() {
    String pattern = "{0}'{'aggregatedValue={1}'}'";
    return MessageFormat.format(pattern, getClass().getSimpleName(), aggregatedValue);
  }
}
