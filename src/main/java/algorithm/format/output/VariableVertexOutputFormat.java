package algorithm.format.output;

import algorithm.format.FormatUtils;
import algorithm.format.vertex.VariableVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import model.identity.UserGroup;
import model.score.SendableRiskScores;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@Log4j2
@NoArgsConstructor
public final class VariableVertexOutputFormat
    extends TextVertexOutputFormat<UserGroup, SendableRiskScores, NullWritable> {

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexOutputFormat.TextVertexWriter createVertexWriter(
      @NonNull TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new VariableVertexWriter(taskAttemptContext);
  }

  @ToString
  private final class VariableVertexWriter extends TextVertexOutputFormat.TextVertexWriter {

    private final RecordWriter<Text, Text> recordReader;

    private VariableVertexWriter(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      recordReader = createLineRecordWriter(taskAttemptContext);
    }

    @Override
    public void writeVertex(Vertex<UserGroup, SendableRiskScores, NullWritable> vertex)
        throws IOException, InterruptedException {
      Text text =
          new Text(OBJECT_MAPPER
              .writeValueAsString(VariableVertex.of(vertex.getId(), vertex.getValue())));
      recordReader.write(text, null);
    }
  }
}
