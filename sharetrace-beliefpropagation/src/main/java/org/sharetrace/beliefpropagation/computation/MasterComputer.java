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
    long superstep = getSuperstep();
    if (HALT_THRESHOLD > delta.get()) {
      LOGGER.debug(
          "Halting computation: aggregated value " + delta.get() + " exceeds " + HALT_THRESHOLD);
      haltComputation();
    } else if (MAX_ITERATIONS <= superstep) {
      LOGGER
          .debug("Halting computation: superstep " + superstep + " exceeds" + MAX_ITERATIONS);
    } else {
      Class<? extends Computation<?, ?, ?, ?, ?>> computation = getVertexComputation(superstep);
      LOGGER.debug("Setting computation to " + computation.getSimpleName());
      setComputation(computation);
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    LOGGER.debug("Initializing VertexValueDeltaAggregator...");
    registerPersistentAggregator(VERTEX_DELTA_AGGREGATOR, VertexValueDeltaAggregator.class);
  }

  public static Instant getInitializedAt() {
    return initializedAt;
  }
}
