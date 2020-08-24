package org.sharetrace.beliefpropagation;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.VertexValueCombiner;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.sharetrace.beliefpropagation.combiner.FactorGraphVertexValueCombiner;
import org.sharetrace.beliefpropagation.computation.MasterComputer;
import org.sharetrace.beliefpropagation.computation.VariableVertexComputation;
import org.sharetrace.beliefpropagation.format.input.FactorGraphVertexInputFormat;
import org.sharetrace.beliefpropagation.format.output.FactorGraphVertexOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BeliefPropagationMain {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeliefPropagationRunner.class);

  private static final Class<? extends VertexInputFormat<?, ?, ?>> VERTEX_INPUT_FORMAT = FactorGraphVertexInputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> VERTEX_OUTPUT_FORMAT = FactorGraphVertexOutputFormat.class;

  private static final Class<? extends VertexValueCombiner<?>> VERTEX_VALUE_COMBINER = FactorGraphVertexValueCombiner.class;

  private static final Class<? extends MasterCompute> MASTER_COMPUTE = MasterComputer.class;

  private static final int MAX_NUM_SUPERSTEPS = 5;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> VARIABLE_VERTEX_COMPUTATION = VariableVertexComputation.class;

  /**
   * @param args Giraph configuration arguments from the command line.
   */
  public static void main(String[] args) throws Exception {
    GiraphConfiguration config = new GiraphConfiguration();
    config.setComputationClass(VARIABLE_VERTEX_COMPUTATION);
    config.setVertexInputFormatClass(VERTEX_INPUT_FORMAT);
    config.setVertexOutputFormatClass(VERTEX_OUTPUT_FORMAT);
    config.setVertexValueCombinerClass(VERTEX_VALUE_COMBINER);
    config.setMasterComputeClass(MASTER_COMPUTE);
    config.setVertexValueCombinerClass(VERTEX_VALUE_COMBINER);
    config.setMaxNumberOfSupersteps(MAX_NUM_SUPERSTEPS);
    config.setLocalTestMode(true);
    config.SPLIT_MASTER_WORKER.set(config, false);
    config.setWorkerConfiguration(1, 1, 100);
    config.setQuietMode(false);

    String inputPath = "~/input.txt";
    String outputPath = "~/out";
    GiraphFileInputFormat.addVertexInputPath(config, new Path(inputPath));
    FileOutputFormat.setOutputPath(new JobConf(config), new Path(outputPath));
    GiraphJob job = new GiraphJob(config, "BeliefPropagation");
    job.run(true);
  }
}
