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
    Preconditions.checkNotNull(context, " TaskAttemptContext must not be null");
    return new FactorGraphVertexWriter();
  }

  private final class FactorGraphVertexWriter extends TextVertexWriterToEachLine {

    @Override
    protected Text convertVertexToLine(
        Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex)
        throws JsonProcessingException {
      Preconditions.checkNotNull(vertex, "Vertex to write must not be null");
      FactorGraphWritable writable = vertex.getValue();
      IdGroup vertexId = vertex.getId().getIdGroup();
      String text;
      if (writable.getType().equals(VertexType.FACTOR)) {
        LOGGER.debug("Vertex to write is a factor vertex");
        FactorVertex factor = FactorVertex.builder()
            .vertexId(vertexId)
            .vertexValue(((FactorVertexValue) writable.getWrapped()).getValue())
            .build();
        LOGGER.debug("Writing factor vertex as a String");
        text = MAPPER.writeValueAsString(factor);
      } else {
        LOGGER.debug("Vertex to write is a variable vertex");
        VariableVertex variable = VariableVertex.builder()
            .vertexId(vertexId)
            .vertexValue(
                ((VariableVertexValue) writable.getWrapped()).getValue())
            .build();
        LOGGER.debug("Writing variable vertex as a String");
        text = MAPPER.writeValueAsString(variable);
      }
      LOGGER.debug("Successfully wrote vertex out");
      return new Text(text);
    }
  }
}
