package sharetrace.algorithm.beliefpropagation.format.input;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
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
import sharetrace.algorithm.beliefpropagation.format.vertex.VariableVertex;
import sharetrace.algorithm.beliefpropagation.format.vertex.VertexType;
import sharetrace.algorithm.beliefpropagation.format.writable.FactorGraphWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.UserGroupWritableComparable;

public class FactorGraphVertexInputFormat extends
    TextVertexInputFormat<UserGroupWritableComparable, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexInputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) {
    return new FactorGraphVertexReader();
  }

  private final class FactorGraphVertexReader extends TextVertexReaderFromEachLine {

    private FactorVertex factorVertex;

    private VariableVertex variableVertex;

    @Override
    protected UserGroupWritableComparable getId(Text line) throws IOException {
      String text = line.toString();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(text));
      JsonParser parser = OBJECT_MAPPER.createParser(text);
      JsonNode node = parser.getCodec().readTree(parser);
      String vertexType = node.get("type").asText();
      UserGroupWritableComparable writableComparable;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        if (factorVertex == null) {
          factorVertex = OBJECT_MAPPER.readValue(text, FactorVertex.class);
        }
        writableComparable = factorVertex.getVertexId().wrap();
      } else {
        if (variableVertex == null) {
          variableVertex = OBJECT_MAPPER.readValue(text, VariableVertex.class);
        }
        writableComparable = variableVertex.getVertexId().wrap();
      }
      return writableComparable;
    }

    @Override
    protected FactorGraphWritable getValue(Text line) throws IOException {
      String text = line.toString();
      Preconditions.checkArgument(!Strings.isNullOrEmpty(text));
      JsonParser parser = OBJECT_MAPPER.createParser(text);
      JsonNode node = parser.getCodec().readTree(parser);
      String vertexType = node.get("type").asText();
      FactorGraphWritable writable;
      if (vertexType.equalsIgnoreCase(VertexType.FACTOR.toString())) {
        if (factorVertex == null) {
          factorVertex = OBJECT_MAPPER.readValue(text, FactorVertex.class);
        }
        writable = factorVertex.getVertexValue().wrap().wrap();
      } else {
        if (variableVertex == null) {
          variableVertex = OBJECT_MAPPER.readValue(text, VariableVertex.class);
        }
        writable = variableVertex.getVertexValue().wrap().wrap();
      }
      return writable;
    }

    @Override
    protected Iterable<Edge<UserGroupWritableComparable, NullWritable>> getEdges(Text line) {
      return null;
    }
  }
}
