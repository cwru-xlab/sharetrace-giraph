package algorithm.format.output;

import algorithm.format.FormatUtils;
import algorithm.format.vertex.VariableVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.text.MessageFormat;
import model.identity.UserGroup;
import model.score.SendableRiskScores;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class VariableVertexOutputFormat
    extends TextVertexOutputFormat<UserGroup, SendableRiskScores, NullWritable> {

  private static final Logger log = LoggerFactory.getLogger(VariableVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(taskAttemptContext);
    return new VariableVertexWriter(taskAttemptContext);
  }

  private final class VariableVertexWriter extends TextVertexWriter {

    private final RecordWriter<Text, Text> recordWriter;

    private VariableVertexWriter(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      recordWriter = createLineRecordWriter(taskAttemptContext);
    }

    @Override
    public void writeVertex(Vertex<UserGroup, SendableRiskScores, NullWritable> vertex)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(vertex);
      VariableVertex variableVertex = VariableVertex.of(vertex.getId(), vertex.getValue());
      Text text = new Text(OBJECT_MAPPER.writeValueAsString(variableVertex));
      recordWriter.write(text, null);
    }

    @Override
    public String toString() {
      return MessageFormat.format("VariableVertexWriter'{'recordWriter={0}'}'", recordWriter);
    }
  }
}
