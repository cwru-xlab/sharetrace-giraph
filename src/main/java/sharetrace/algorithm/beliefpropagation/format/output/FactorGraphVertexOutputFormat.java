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
import sharetrace.algorithm.beliefpropagation.format.vertex.VariableVertex;
import sharetrace.algorithm.beliefpropagation.format.vertex.VertexType;
import sharetrace.algorithm.beliefpropagation.format.writable.ContactWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.FactorGraphWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.SendableRiskScoresWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.UserGroupWritableComparable;
import sharetrace.model.identity.UserGroup;

public class FactorGraphVertexOutputFormat extends
    TextVertexOutputFormat<UserGroupWritableComparable, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexOutputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    Preconditions.checkNotNull(context);
    return new FactorGraphVertexWriter();
  }

  private final class FactorGraphVertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<UserGroupWritableComparable, FactorGraphWritable, NullWritable> vertex)
        throws JsonProcessingException {
      Preconditions.checkNotNull(vertex);
      FactorGraphWritable writable = vertex.getValue();
      UserGroup vertexId = vertex.getId().getUserGroup();
      String text;
      if (writable.getType().equals(VertexType.FACTOR)) {
        FactorVertex factor = FactorVertex.builder()
            .setVertexId(vertexId)
            .setVertexValue(((ContactWritable) writable.getWrapped()).getContact())
            .build();
        text = OBJECT_MAPPER.writeValueAsString(factor);
      } else {
        VariableVertex variable = VariableVertex.builder()
            .setVertexId(vertexId)
            .setVertexValue(
                ((SendableRiskScoresWritable) writable.getWrapped()).getSendableRiskScores())
            .build();
        text = OBJECT_MAPPER.writeValueAsString(variable);
      }
      return new Text(text);
    }
  }
}
