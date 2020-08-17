package org.sharetrace.beliefpropagation;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.sharetrace.beliefpropagation.combiner.FactorGraphVertexValueCombiner;
import org.sharetrace.beliefpropagation.computation.FactorVertexComputation;
import org.sharetrace.beliefpropagation.computation.MasterComputer;
import org.sharetrace.beliefpropagation.computation.VariableVertexComputation;
import org.sharetrace.beliefpropagation.format.input.FactorGraphVertexInputFormat;
import org.sharetrace.beliefpropagation.format.output.FactorGraphVertexOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * For a full list of available configuration options, refer to the following link:
 * https://giraph.apache.org/apidocs/org/apache/giraph/conf/GiraphConfiguration.html
 */
public final class BeliefPropagationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeliefPropagationRunner.class);

  private static final Class<? extends VertexInputFormat<?, ?, ?>> VERTEX_INPUT_FORMAT = FactorGraphVertexInputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> VERTEX_OUTPUT_FORMAT = FactorGraphVertexOutputFormat.class;

  private static final Class<? extends VertexValueCombiner<?>> VERTEX_VALUE_COMBINER = FactorGraphVertexValueCombiner.class;

  private static final Class<? extends MasterCompute> MASTER_COMPUTE = MasterComputer.class;

  private static final int MAX_NUM_SUPERSTEPS = 5;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> FACTOR_VERTEX_COMPUTATION = FactorVertexComputation.class;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> VARIABLE_VERTEX_COMPUTATION = VariableVertexComputation.class;

  private BeliefPropagationRunner() {
  }

  /**
   * @param args Giraph configuration arguments from the command line.
   */
  public static void main(String[] args) throws Exception {
    GiraphConfiguration config = new GiraphConfiguration();
    config.setVertexInputFormatClass(VERTEX_INPUT_FORMAT);
    config.setVertexOutputFormatClass(VERTEX_OUTPUT_FORMAT);
    config.setVertexValueCombinerClass(VERTEX_VALUE_COMBINER);
    config.setMasterComputeClass(MASTER_COMPUTE);
    config.setVertexValueCombinerClass(VERTEX_VALUE_COMBINER);
    config.setMaxNumberOfSupersteps(MAX_NUM_SUPERSTEPS);
    config.COMPUTATION_CLASS
        .setMany(config, FACTOR_VERTEX_COMPUTATION, VARIABLE_VERTEX_COMPUTATION);
    Tool runner = new GiraphRunner();
    runner.setConf(config);
    try {
      ToolRunner.run(runner, args);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }
}
