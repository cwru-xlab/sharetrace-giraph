package sharetrace.algorithm.beliefpropagation.format.input;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import sharetrace.algorithm.beliefpropagation.format.vertex.FactorVertex;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.identity.UserGroupWritableComparable;

public class FactorVertexInputFormat extends
    TextVertexInputFormat<UserGroupWritableComparable, ContactWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexInputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public final TextVertexReaderFromEachLine createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new FactorVertexReader();
  }

  private final class FactorVertexReader extends TextVertexReaderFromEachLine {

    @Override
    protected UserGroupWritableComparable getId(Text line) throws IOException {
      return OBJECT_MAPPER
          .readValue(line.toString(), FactorVertex.class)
          .getVertexId()
          .wrap();
    }

    @Override
    protected ContactWritable getValue(Text line) throws IOException {
      return OBJECT_MAPPER
          .readValue(line.toString(), FactorVertex.class)
          .getVertexValue()
          .wrap();
    }

    @Override
    protected Iterable<Edge<UserGroupWritableComparable, NullWritable>> getEdges(Text line) {
      return null;
    }
  }
}
