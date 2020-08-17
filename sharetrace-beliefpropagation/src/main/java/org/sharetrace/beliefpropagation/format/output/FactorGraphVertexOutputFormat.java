package org.sharetrace.beliefpropagation.format.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sharetrace.beliefpropagation.format.FormatUtils;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.vertex.FactorVertex;
import org.sharetrace.model.vertex.VariableVertex;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactorGraphVertexOutputFormat extends
    TextVertexOutputFormat<FactorGraphVertexId, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexOutputFormat.class);

  private static final ObjectMapper MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    Preconditions.checkNotNull(context);
    return new FactorGraphVertexWriter();
  }

  private final class FactorGraphVertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex)
        throws JsonProcessingException {
      Preconditions.checkNotNull(vertex);
      FactorGraphWritable writable = vertex.getValue();
      IdGroup vertexId = vertex.getId().getIdGroup();
      String text;
      if (writable.getType().equals(VertexType.FACTOR)) {
        FactorVertex factor = FactorVertex.builder()
            .setVertexId(vertexId)
            .setVertexValue(((FactorVertexValue) writable.getWrapped()).getContact())
            .build();
        text = MAPPER.writeValueAsString(factor);
      } else {
        VariableVertex variable = VariableVertex.builder()
            .setVertexId(vertexId)
            .setVertexValue(
                ((VariableVertexValue) writable.getWrapped()).getSendableRiskScores())
            .build();
        text = MAPPER.writeValueAsString(variable);
      }
      return new Text(text);
    }
  }
}
