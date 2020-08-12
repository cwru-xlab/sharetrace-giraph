package sharetrace.algorithm.beliefpropagation.format.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.format.FormatUtils;
import sharetrace.algorithm.beliefpropagation.format.vertex.VariableVertex;
import sharetrace.model.identity.UserGroupWritableComparable;
import sharetrace.model.score.SendableRiskScoresWritable;

public class VariableVertexInputFormat extends
    TextVertexInputFormat<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VariableVertexInputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public final TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
    return new VariableVertexReader();
  }

  private final class VariableVertexReader extends TextVertexReader {

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      Text line = getRecordReader().getCurrentValue();
      VariableVertex variableVertex = OBJECT_MAPPER
          .readValue(line.toString(), VariableVertex.class);
      Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex;
      vertex = getConf().createVertex();
      vertex.initialize(variableVertex.getVertexId().wrap(),
          variableVertex.getVertexValue().wrap());
      return vertex;
    }
  }
}
