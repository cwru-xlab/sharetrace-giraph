package algorithm.format.output;

import algorithm.format.FormatUtils;
import algorithm.format.vertex.FactorVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.text.MessageFormat;
import model.contact.Contact;
import model.identity.UserGroup;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FactorVertexOutputFormat extends
    TextVertexOutputFormat<UserGroup, Contact, NullWritable> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(taskAttemptContext);
    return new FactorVertexWriter(taskAttemptContext);
  }

  private final class FactorVertexWriter extends TextVertexWriter {

    private final RecordWriter<Text, Text> recordWriter;

    private FactorVertexWriter(TaskAttemptContext taskAttemptContext)
        throws IOException, InterruptedException {
      recordWriter = createLineRecordWriter(taskAttemptContext);
    }

    @Override
    public void writeVertex(Vertex<UserGroup, Contact, NullWritable> vertex)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(vertex);
      FactorVertex factorVertex = FactorVertex.of(vertex.getId(), vertex.getValue());
      Text text = new Text(OBJECT_MAPPER.writeValueAsString(factorVertex));
      recordWriter.write(text, null);
    }

    @Override
    public String toString() {
      return MessageFormat.format("FactorVertexWriter'{'recordWriter={0}'}'", recordWriter);
    }
  }
}
