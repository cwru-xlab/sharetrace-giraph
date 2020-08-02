package algorithm.main;

import algorithm.computation.FactorVertexComputation;
import algorithm.computation.MasterComputer;
import algorithm.computation.VariableVertexComputation;
import algorithm.format.input.FactorVertexInputFormat;
import algorithm.format.input.VariableVertexInputFormat;
import algorithm.format.output.FactorVertexOutputFormat;
import algorithm.format.output.VariableVertexOutputFormat;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import model.contact.Contact;
import model.identity.UserGroup;
import model.score.SendableRiskScores;
import model.score.TemporalUserRiskScore;
import org.apache.giraph.GiraphRunner;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.io.VertexInputFormat;
import org.apache.giraph.io.VertexOutputFormat;
import org.apache.giraph.io.formats.GiraphFileInputFormat;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * For a full list of available configuration options, refer to the following link:
 * https://giraph.apache.org/apidocs/org/apache/giraph/conf/GiraphConfiguration.html
 */
@Log4j2
@ToString
@RequiredArgsConstructor
public final class BeliefPropagationRunner {

  private static final boolean IS_STATIC_GRAPH = true;

  private static final boolean USE_MESSAGE_SIZE_ENCODING = true;

  private static final boolean IS_VERTEX_OUTPUT_FORMAT_THREAD_SAFE = true;

  private static final Class<? extends VertexInputFormat<?, ?, ?>> FACTOR_VERTEX_INPUT_FORMAT =
      FactorVertexInputFormat.class;

  private static final Class<? extends VertexInputFormat<?, ?, ?>> VARIABLE_VERTEX_INPUT_FORMAT =
      VariableVertexInputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> FACTOR_VERTEX_OUTPUT_FORMAT =
      FactorVertexOutputFormat.class;

  private static final Class<? extends VertexOutputFormat<?, ?, ?>> VARIABLE_VERTEX_OUTPUT_FORMAT =
      VariableVertexOutputFormat.class;

  private static final Class<? extends WritableComparable<?>> VERTEX_ID = UserGroup.class;

  private static final Class<? extends Writable> FACTOR_VERTEX_VALUE = Contact.class;

  private static final Class<? extends Writable> VARIABLE_VERTEX_VALUE = TemporalUserRiskScore.class;

  private static final Class<? extends Writable> OUTGOING_MESSAGE_VALUE = SendableRiskScores.class;

  private static final Class<? extends MasterCompute> MASTER_COMPUTE = MasterComputer.class;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> FACTOR_VERTEX_COMPUTATION =
      FactorVertexComputation.class;

  private static final Class<? extends AbstractComputation<?, ?, ?, ?, ?>> VARIABLE_VERTEX_COMPUTATION =
      VariableVertexComputation.class;

  private static final String FACTOR_VERTEX_INPUT_PATH = "/bp/in/factor.txt";

  private static final String VARIABLE_VERTEX_INPUT_PATH = "/bp/in/variable.txt";

  private static final String OUTPUT_PATH = "/bp/out";

  /**
   * @param args Additional Giraph configuration arguments from the command line.
   */
  public static void main(String[] args) throws Exception {
    GiraphConfiguration config = new GiraphConfiguration();
    // Memory/storage optimization
    config.STATIC_GRAPH.set(config, IS_STATIC_GRAPH);
    config.USE_MESSAGE_SIZE_ENCODING.set(config, USE_MESSAGE_SIZE_ENCODING);
    // I/O
    GiraphFileInputFormat.addVertexInputPath(config, new Path(FACTOR_VERTEX_INPUT_PATH));
    GiraphFileInputFormat.addVertexInputPath(config, new Path(VARIABLE_VERTEX_INPUT_PATH));
    FileOutputFormat.setOutputPath(new JobConf(config), new Path(OUTPUT_PATH));
    config.VERTEX_INPUT_FORMAT_CLASS
        .setMany(config, FACTOR_VERTEX_INPUT_FORMAT, VARIABLE_VERTEX_INPUT_FORMAT);
    config.VERTEX_OUTPUT_FORMAT_CLASS
        .setMany(config, FACTOR_VERTEX_OUTPUT_FORMAT, VARIABLE_VERTEX_OUTPUT_FORMAT);
    config.VERTEX_OUTPUT_FORMAT_THREAD_SAFE.set(config, IS_VERTEX_OUTPUT_FORMAT_THREAD_SAFE);
    // Vertex types
    config.VERTEX_ID_CLASS.set(config, VERTEX_ID);
    config.VERTEX_VALUE_CLASS.setMany(config, FACTOR_VERTEX_VALUE, VARIABLE_VERTEX_VALUE);
    // Message types
    config.OUTGOING_MESSAGE_VALUE_CLASS.set(config, OUTGOING_MESSAGE_VALUE);
    // Computation
    config.MASTER_COMPUTE_CLASS.set(config, MASTER_COMPUTE);
    config.COMPUTATION_CLASS
        .setMany(config, FACTOR_VERTEX_COMPUTATION, VARIABLE_VERTEX_COMPUTATION);
    // Run
    Tool runner = new GiraphRunner();
    runner.setConf(config);
    ToolRunner.run(runner, args);
  }
}
