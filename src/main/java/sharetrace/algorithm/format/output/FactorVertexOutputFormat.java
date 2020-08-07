package sharetrace.algorithm.format.output;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.text.MessageFormat;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.format.FormatUtils;
import sharetrace.algorithm.format.vertex.FactorVertex;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.identity.UserGroupWritableComparable;

public final class FactorVertexOutputFormat extends
    TextVertexOutputFormat<UserGroupWritableComparable, ContactWritable, NullWritable> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    Preconditions.checkNotNull(context);
    return new FactorVertexWriter(context);
  }

  private final class FactorVertexWriter extends TextVertexWriter {

    private final RecordWriter<Text, Text> recordWriter;

    private FactorVertexWriter(TaskAttemptContext context)
        throws IOException, InterruptedException {
      recordWriter = createLineRecordWriter(context);
    }

    @Override
    public void writeVertex(
        Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex)
        throws IOException, InterruptedException {
      Preconditions.checkNotNull(vertex);
      FactorVertex factorVertex = FactorVertex.builder()
          .setVertexId(vertex.getId().getUserGroup())
          .setVertexValue(vertex.getValue().getContact())
          .build();
      Text text = new Text(OBJECT_MAPPER.writeValueAsString(factorVertex));
      recordWriter.write(text, null);
    }

    @Override
    public String toString() {
      return MessageFormat.format("{0}'{'recordWriter={1}'}'",
          getClass().getSimpleName(),
          recordWriter);
    }
  }
}
