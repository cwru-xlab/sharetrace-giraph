package algorithm.aggregators;

import algorithm.computation.MasterComputer;
import com.google.common.base.Preconditions;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.hadoop.io.DoubleWritable;

@Log4j2
public final class VertexValueDeltaAggregator implements Aggregator<DoubleWritable> {

  private static final Double INITIAL_VALUE = Double.POSITIVE_INFINITY;

  private static final String AGGREGATOR_NAME = MasterComputer.getVertexDeltaAggregatorName();

  private DoubleWritable aggregatedValue = new DoubleWritable(INITIAL_VALUE);


  

  @Override
  public void aggregate(DoubleWritable a) {
    Preconditions.checkNotNull(a);
    setAggregatedValue(new DoubleWritable(aggregatedValue.get() + a.get()));
  }

  @Override
  public DoubleWritable createInitialValue() {
    return new DoubleWritable(INITIAL_VALUE);
  }

  @Override
  public DoubleWritable getAggregatedValue() {
    return aggregatedValue;
  }

  @Override
  public void setAggregatedValue(DoubleWritable a) {
    Preconditions.checkNotNull(a);
    aggregatedValue.set(a.get());
  }

  @Override
  public void reset() {
    Optional.ofNullable(aggregatedValue)
        .ifPresentOrElse(v -> aggregatedValue = new DoubleWritable(INITIAL_VALUE),
            () -> aggregatedValue.set(INITIAL_VALUE));
  }
}
