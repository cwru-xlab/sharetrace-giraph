package org.sharetrace.beliefpropagation.compute;

import java.text.MessageFormat;
import java.time.Instant;
import org.apache.giraph.graph.Computation;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.sharetrace.beliefpropagation.aggregate.DeltaAggregator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MasterComputer extends DefaultMasterCompute {

  // Logging messages
  private static final String VALUE_HALT_MSG = "Halting computation: aggregated value {0} exceeds {1}";
  private static final String SUPERSTEP_HALT_MSG = "Halting computation: superstep {0} exceeds {1}";
  private static final String SETTING_COMPUTE_MSG = "Setting computation to {0}";
  private static final String INITIALIZING_AGG_MSG = "Initializing VertexValueDeltaAggregator...";

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
      LOGGER.debug(MessageFormat.format(VALUE_HALT_MSG, delta.get(), HALT_THRESHOLD));
      haltComputation();
    } else if (MAX_ITERATIONS <= superstep) {
      LOGGER.debug(MessageFormat.format(SUPERSTEP_HALT_MSG, superstep, MAX_ITERATIONS));
    } else {
      Class<? extends Computation<?, ?, ?, ?, ?>> computation = getVertexComputation(superstep);
      LOGGER.debug(MessageFormat.format(SETTING_COMPUTE_MSG, computation.getSimpleName()));
      setComputation(computation);
    }
  }

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    LOGGER.debug(INITIALIZING_AGG_MSG);
    registerPersistentAggregator(VERTEX_DELTA_AGGREGATOR, DeltaAggregator.class);
  }

  public static Instant getInitializedAt() {
    return initializedAt;
  }
}
