package sharetrace.algorithm.beliefpropagation.format.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.format.FormatUtils;
import sharetrace.model.identity.UserGroupWritableComparable;
import sharetrace.model.score.SendableRiskScoresWritable;

public final class VariableVertexOutputFormat extends
    TextVertexOutputFormat<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(VariableVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriterToEachLine createVertexWriter(TaskAttemptContext context) {
    Preconditions.checkNotNull(context);
    return new VariableVertexWriter();
  }

  private final class VariableVertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex)
        throws JsonProcessingException {
      return new Text(OBJECT_MAPPER.writeValueAsString(vertex.getValue().getMaxRiskScore()));
    }
  }
}
