package sharetrace.algorithm.beliefpropagation.main;

import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.computation.FactorVertexComputation;
import sharetrace.algorithm.beliefpropagation.computation.MasterComputer;
import sharetrace.algorithm.beliefpropagation.computation.VariableVertexComputation;
import sharetrace.algorithm.beliefpropagation.filter.ExpiredFactorVertexFilter;
import sharetrace.algorithm.beliefpropagation.format.input.FactorVertexInputFormat;
import sharetrace.algorithm.beliefpropagation.format.input.VariableVertexInputFormat;
import sharetrace.algorithm.beliefpropagation.format.output.FactorVertexOutputFormat;
import sharetrace.algorithm.beliefpropagation.format.output.VariableVertexOutputFormat;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.identity.UserGroupWritableComparable;
import sharetrace.model.score.SendableRiskScoresWritable;

/**
 * For a full list of available configuration options, refer to the following link:
 * https://giraph.apache.org/apidocs/org/apache/giraph/conf/GiraphConfiguration.html
 */
public final class BeliefPropagationRunner {

  private static final Logger LOGGER = LoggerFactory.getLogger(BeliefPropagationRunner.class);

  private static final Class<? extends VertexInputFormat<?, ?, ?>> FACTOR_VERTEX_INPUT_FORMAT =
      FactorVertexInputFormat.class;

  private static final Class<? extends VertexInputFormat<?, ?, ?>> VARIABLE_VERTEX_INPUT_FORMAT =
      VariableVertexInputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> FACTOR_VERTEX_OUTPUT_FORMAT =
      FactorVertexOutputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> VARIABLE_VERTEX_OUTPUT_FORMAT =
      VariableVertexOutputFormat.class;

  private static final Class<? extends VertexInputFilter<?, ?, ?>> FACTOR_VERTEX_INPUT_FILTER =
      ExpiredFactorVertexFilter.class;

  private static final Class<? extends WritableComparable<?>> VERTEX_ID = UserGroupWritableComparable.class;

  private static final Class<? extends Writable> FACTOR_VERTEX_VALUE = ContactWritable.class;

  private static final Class<? extends Writable> VARIABLE_VERTEX_VALUE = SendableRiskScoresWritable.class;

  private static final Class<? extends Writable> OUTGOING_MESSAGE_VALUE = SendableRiskScoresWritable.class;

  private static final Class<? extends MasterCompute> MASTER_COMPUTE = MasterComputer.class;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> FACTOR_VERTEX_COMPUTATION =
      FactorVertexComputation.class;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> VARIABLE_VERTEX_COMPUTATION =
      VariableVertexComputation.class;

  private static final String FACTOR_VERTEX_INPUT_PATH = "/bp/in/factor.txt";

  private static final String VARIABLE_VERTEX_INPUT_PATH = "/bp/in/variable.txt";

  private static final String OUTPUT_PATH = "/bp/out";

  private BeliefPropagationRunner() {
  }

  /**
   * @param args Additional Giraph configuration arguments from the command line.
   */
  public static void main(String[] args) throws Exception {
    GiraphConfiguration config = new GiraphConfiguration();
    GiraphFileInputFormat.addVertexInputPath(config, new Path(FACTOR_VERTEX_INPUT_PATH));
    GiraphFileInputFormat.addVertexInputPath(config, new Path(VARIABLE_VERTEX_INPUT_PATH));
    FileOutputFormat.setOutputPath(new JobConf(config), new Path(OUTPUT_PATH));
    config.VERTEX_INPUT_FORMAT_CLASS
        .setMany(config, FACTOR_VERTEX_INPUT_FORMAT, VARIABLE_VERTEX_INPUT_FORMAT);
    config.VERTEX_OUTPUT_FORMAT_CLASS
        .setMany(config, FACTOR_VERTEX_OUTPUT_FORMAT, VARIABLE_VERTEX_OUTPUT_FORMAT);
    config.VERTEX_INPUT_FILTER_CLASS.set(config, FACTOR_VERTEX_INPUT_FILTER);
    config.VERTEX_ID_CLASS.set(config, VERTEX_ID);
    config.VERTEX_VALUE_CLASS.setMany(config, FACTOR_VERTEX_VALUE, VARIABLE_VERTEX_VALUE);
    config.OUTGOING_MESSAGE_VALUE_CLASS.set(config, OUTGOING_MESSAGE_VALUE);
    config.MASTER_COMPUTE_CLASS.set(config, MASTER_COMPUTE);
    config.COMPUTATION_CLASS
        .setMany(config, FACTOR_VERTEX_COMPUTATION, VARIABLE_VERTEX_COMPUTATION);
    Tool runner = new GiraphRunner();
    runner.setConf(config);
    ToolRunner.run(runner, args);
  }
}