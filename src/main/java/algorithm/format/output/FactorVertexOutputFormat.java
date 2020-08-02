package algorithm.format.output;

import algorithm.format.FormatUtils;
import algorithm.format.vertex.FactorVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;
import model.contact.Contact;
import model.identity.UserGroup;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

@Log4j2
@NoArgsConstructor
public final class FactorVertexOutputFormat extends
    TextVertexOutputFormat<UserGroup, Contact, NullWritable> {

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexOutputFormat.TextVertexWriter createVertexWriter(
      @NonNull TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new FactorVertexWriter(taskAttemptContext);
  }

  @ToString
  private final class FactorVertexWriter extends TextVertexOutputFormat.TextVertexWriter {

    private final RecordWriter<Text, Text> recordWriter;

    private FactorVertexWriter(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      recordWriter = createLineRecordWriter(taskAttemptContext);
    }

    @Override
    public void writeVertex(Vertex<UserGroup, Contact, NullWritable> vertex)
        throws IOException, InterruptedException {
      Text text = new Text(
          OBJECT_MAPPER.writeValueAsString(FactorVertex.of(vertex.getId(), vertex.getValue())));
      recordWriter.write(text, null);
    }
  }
}
