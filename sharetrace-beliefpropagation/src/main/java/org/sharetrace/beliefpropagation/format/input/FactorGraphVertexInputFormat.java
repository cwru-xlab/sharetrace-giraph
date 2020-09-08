package org.sharetrace.beliefpropagation.format.input;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.model.vertex.FactorVertex;
import org.sharetrace.model.vertex.VariableVertex;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactorGraphVertexInputFormat extends
    TextVertexInputFormat<FactorGraphVertexId, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexInputFormat.class);

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final String TYPE = "type";

  @Override
  public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
    return new FactorGraphVertexReader();
  }

  private final class FactorGraphVertexReader extends TextVertexReaderFromEachLine {

    private FactorVertex factorVertex;

    private VariableVertex variableVertex;

    @Override
    protected FactorGraphVertexId getId(Text line) throws IOException {
      Preconditions.checkNotNull(line);
      String text = line.toString();
      String vertexType = getVertexType(text);
      FactorGraphVertexId vertexId;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        setFactorVertex(text);
        vertexId = FactorGraphVertexId.of(factorVertex.getVertexId());
      } else {
        setVariableVertex(text);
        vertexId = FactorGraphVertexId.of(variableVertex.getVertexId());
      }
      return vertexId;
    }

    @Override
    protected FactorGraphWritable getValue(Text line) throws IOException {
      Preconditions.checkNotNull(line);
      String text = line.toString();
      String vertexType = getVertexType(text);
      FactorGraphWritable writable;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        setFactorVertex(text);
        FactorVertexValue value = FactorVertexValue.of(factorVertex.getVertexValue());
        writable = FactorGraphWritable.ofFactorVertex(value);
      } else {
        setVariableVertex(text);
        VariableVertexValue value = VariableVertexValue.of(variableVertex.getVertexValue());
        writable = FactorGraphWritable.ofVariableVertex(value);
      }
      return writable;
    }

    @Override
    protected Iterable<Edge<FactorGraphVertexId, NullWritable>> getEdges(Text line) {
      return null;
    }

    private String getVertexType(String input) throws IOException {
      JsonFactory factory = MAPPER.getFactory();
      JsonParser parser = factory.createParser(input);
      JsonNode node = parser.getCodec().readTree(parser);
      return node.get(TYPE).asText();
    }

    private void setFactorVertex(String vertex) throws JsonProcessingException {
      if (factorVertex == null) {
        factorVertex = MAPPER.readValue(vertex, FactorVertex.class);
      }
    }

    private void setVariableVertex(String vertex) throws JsonProcessingException {
      if (variableVertex == null) {
        variableVertex = MAPPER.readValue(vertex, VariableVertex.class);
      }
    }
  }


}
