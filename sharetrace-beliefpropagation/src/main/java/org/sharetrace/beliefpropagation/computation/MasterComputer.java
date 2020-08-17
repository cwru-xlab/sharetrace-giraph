package org.sharetrace.beliefpropagation.computation;

import java.time.Instant;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.sharetrace.beliefpropagation.aggregators.VertexValueDeltaAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MasterComputer extends DefaultMasterCompute {

  private static final Logger LOGGER = LoggerFactory.getLogger(MasterComputer.class);

  private static final Instant initializedAt = Instant.now();

  private static final double HALT_THRESHOLD = 0.00001;

  private static final long MAX_ITERATIONS = 5L;

  private static final String VERTEX_DELTA_AGGREGATOR = "vertexDeltaAggregator";

  private static Class<? extends Computation<?, ?, ?, ?, ?>> getVertexComputation(long superStep) {
    return isEven(superStep) ? VariableVertexComputation.class : FactorVertexComputation.class;
  }

  private static boolean isEven(long l) {
    return 0 == Math.floorMod(l, 2);
  }

  static String getVertexDeltaAggregatorName() {
    return VERTEX_DELTA_AGGREGATOR;
  }

  @Override
  public void compute() {
    DoubleWritable delta = getAggregatedValue(VERTEX_DELTA_AGGREGATOR);
    if (HALT_THRESHOLD > delta.get() || MAX_ITERATIONS <= getSuperstep()) {
      haltComputation();
    } else {
      setComputation(getVertexComputation(getSuperstep()));
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    registerPersistentAggregator(VERTEX_DELTA_AGGREGATOR, VertexValueDeltaAggregator.class);
  }

  public static Instant getInitializedAt() {
    return initializedAt;
  }
}