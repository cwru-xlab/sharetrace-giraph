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
import sharetrace.algorithm.beliefpropagation.format.vertex.FactorVertex;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.identity.UserGroupWritableComparable;

public class FactorVertexInputFormat extends
    TextVertexInputFormat<UserGroupWritableComparable, ContactWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexInputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public final TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
    return new FactorVertexReader();
  }

  private final class FactorVertexReader extends TextVertexReader {

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      Text line = getRecordReader().getCurrentValue();
      FactorVertex factorVertex = OBJECT_MAPPER.readValue(line.toString(), FactorVertex.class);
      Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex;
      vertex = getConf().createVertex();
      vertex.initialize(factorVertex.getVertexId().wrap(), factorVertex.getVertexValue().wrap());
      return vertex;
    }
  }
}
