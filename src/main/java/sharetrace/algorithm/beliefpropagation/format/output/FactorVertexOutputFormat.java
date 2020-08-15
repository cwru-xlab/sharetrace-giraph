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
import sharetrace.algorithm.beliefpropagation.format.vertex.FactorVertex;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.identity.UserGroupWritableComparable;

public final class FactorVertexOutputFormat extends
    TextVertexOutputFormat<UserGroupWritableComparable, ContactWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriterToEachLine createVertexWriter(TaskAttemptContext context) {
    Preconditions.checkNotNull(context);
    return new FactorVertexWriter();
  }

  private final class FactorVertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex)
        throws JsonProcessingException {
      Preconditions.checkNotNull(vertex);
      FactorVertex factorVertex = FactorVertex.builder()
          .setVertexId(vertex.getId().getUserGroup())
          .setVertexValue(vertex.getValue().getContact())
          .build();
      return new Text(OBJECT_MAPPER.writeValueAsString(factorVertex));
    }
  }
}
