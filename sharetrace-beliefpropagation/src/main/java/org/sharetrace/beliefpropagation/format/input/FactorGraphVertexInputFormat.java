package org.sharetrace.beliefpropagation.format.input;

import com.fasterxml.jackson.core.JsonParser;
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
import org.sharetrace.beliefpropagation.format.FormatUtils;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.vertex.FactorVertex;
import org.sharetrace.model.vertex.VariableVertex;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactorGraphVertexInputFormat extends
    TextVertexInputFormat<FactorGraphVertexId, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexInputFormat.class);

  private static final ObjectMapper MAPPER = FormatUtils.getObjectMapper();

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
      Preconditions.checkNotNull(line, "Vertex id text must not be null");
      String text = line.toString();
      LOGGER.debug("Parsing vertex id text...");
      JsonParser parser = MAPPER.createParser(text);
      JsonNode node = parser.getCodec().readTree(parser);
      LOGGER.debug("Getting the type of the vertex...");
      String vertexType = node.get(TYPE).asText();
      FactorGraphVertexId vertexId;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        LOGGER.debug("Vertex text is a factor vertex");
        if (factorVertex == null) {
          factorVertex = MAPPER.readValue(text, FactorVertex.class);
        }
        vertexId = FactorGraphVertexId.of(factorVertex.getVertexId());
      } else {
        LOGGER.debug("Vertex text is a variable vertex");
        if (variableVertex == null) {
          variableVertex = MAPPER.readValue(text, VariableVertex.class);
        }
        vertexId = FactorGraphVertexId.of(variableVertex.getVertexId());
      }
      LOGGER.debug("Vertex id has been successfully read");
      return vertexId;
    }

    @Override
    protected FactorGraphWritable getValue(Text line) throws IOException {
      Preconditions.checkNotNull(line, "Vertex value text must not be null");
      String text = line.toString();
      LOGGER.debug("Parsing vertex value text...");
      JsonParser parser = MAPPER.createParser(text);
      JsonNode node = parser.getCodec().readTree(parser);
      LOGGER.debug("Getting the type of the vertex...");
      String vertexType = node.get(TYPE).asText();
      FactorGraphWritable writable;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        LOGGER.debug("Vertex text is a factor vertex");
        if (factorVertex == null) {
          factorVertex = MAPPER.readValue(text, FactorVertex.class);
        }
        writable = FactorGraphWritable
            .ofFactorVertex(FactorVertexValue.of(factorVertex.getVertexValue()));
      } else {
        LOGGER.debug("Vertex text is a variable vertex");
        if (variableVertex == null) {
          variableVertex = MAPPER.readValue(text, VariableVertex.class);
        }
        writable = FactorGraphWritable
            .ofVariableVertex(VariableVertexValue.of(variableVertex.getVertexValue()));
      }
      LOGGER.debug("Vertex value has been successfully read");
      return writable;
    }

    @Override
    protected Iterable<Edge<FactorGraphVertexId, NullWritable>> getEdges(Text line) {
      return null;
    }
  }
}
