package org.sharetrace.beliefpropagation.aggregate;

import com.google.common.base.Preconditions;
import java.text.MessageFormat;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.DoubleWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class DeltaAggregator implements Aggregator<DoubleWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeltaAggregator.class);

  private static final String TO_STRING_PATTERN = "{0}'{'aggregatedValue={1}'}'";

  private static final Double INITIAL_VALUE = Double.POSITIVE_INFINITY;

  private DoubleWritable aggregatedValue = new DoubleWritable(INITIAL_VALUE);

  @Override
  public void aggregate(DoubleWritable a) {
    Preconditions.checkNotNull(a);
    Double prev = aggregatedValue.get();
    double val = a.get();
    double newVal;
    if (prev.equals(Double.POSITIVE_INFINITY)) {
      newVal = val;
    } else {
      newVal = prev + val;
    }
    setAggregatedValue(new DoubleWritable(newVal));
  }

  @Override
  public DoubleWritable createInitialValue() {
    return new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public DoubleWritable getAggregatedValue() {
    return new DoubleWritable(aggregatedValue.get());
  }

  @Override
  public void setAggregatedValue(DoubleWritable a) {
    Preconditions.checkNotNull(a);
    aggregatedValue.set(a.get());
  }

  @Override
  public void reset() {
    aggregatedValue = new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public String toString() {
    return MessageFormat.format(TO_STRING_PATTERN, getClass().getSimpleName(), aggregatedValue);
  }
}
