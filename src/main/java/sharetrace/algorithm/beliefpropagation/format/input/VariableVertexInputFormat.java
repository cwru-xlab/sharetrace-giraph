package sharetrace.algorithm.beliefpropagation.format.input;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
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
  public final TextVertexReaderFromEachLine createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new VariableVertexReader();
  }

  private final class VariableVertexReader extends TextVertexReaderFromEachLine {

    @Override
    protected UserGroupWritableComparable getId(Text line) throws IOException {
      Preconditions.checkNotNull(line);
      return OBJECT_MAPPER
          .readValue(line.toString(), VariableVertex.class)
          .getVertexId()
          .wrap();
    }

    @Override
    protected SendableRiskScoresWritable getValue(Text line) throws IOException {
      Preconditions.checkNotNull(line);
      return OBJECT_MAPPER
          .readValue(line.toString(), VariableVertex.class)
          .getVertexValue()
          .wrap();
    }

    @Override
    protected Iterable<Edge<UserGroupWritableComparable, NullWritable>> getEdges(Text line) {
      return null;
    }
  }
}
