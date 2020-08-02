package algorithm.computation;

import algorithm.aggregators.VertexValueDeltaAggregator;
import lombok.extern.log4j.Log4j2;
import model.identity.UserGroup;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;

@Log4j2
public final class MasterComputer extends DefaultMasterCompute {

  private static final double HALT_THRESHOLD = 0.001;

  private static final long MAX_ITERATIONS = 10L;

  private static final String VERTEX_DELTA_AGGREGATOR = "vertexDeltaAggregator";

  private static boolean isLessThanHaltThreshold(double delta) {
    return HALT_THRESHOLD > delta;
  }

  private static Class<? extends Computation<UserGroup, ?, NullWritable, ?, ?>> getVertexComputation(
      long superStep) {
    return 0 == Math.floorMod(superStep, 2) ? VariableVertexComputation.class
        : FactorVertexComputation.class;
  }

  public static String getVertexDeltaAggregatorName() {
    return VERTEX_DELTA_AGGREGATOR;
  }

  @Override
  public void compute() {
    DoubleWritable delta = getAggregatedValue(VERTEX_DELTA_AGGREGATOR);
    if (isLessThanHaltThreshold(delta.get()) || isGreaterThanMaxIterations()) {
      haltComputation();
    } else {
      setComputation(getVertexComputation(getSuperstep()));
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    registerPersistentAggregator(VERTEX_DELTA_AGGREGATOR, VertexValueDeltaAggregator.class);
  }

  private boolean isGreaterThanMaxIterations() {
    return MAX_ITERATIONS <= getSuperstep();
  }
}
