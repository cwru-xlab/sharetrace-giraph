package algorithm.format.input;

import algorithm.format.FormatUtils;
import algorithm.format.vertex.VariableVertex;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import model.identity.UserGroup;
import model.score.SendableRiskScores;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VariableVertexInputFormat extends
    TextVertexInputFormat<UserGroup, SendableRiskScores, NullWritable> {

  private static final Logger log = LoggerFactory.getLogger(VariableVertexInputFormat.class);

  private static final ObjectMapper OBJECT_MAPPER = FormatUtils.getObjectMapper();

  @Override
  public final TextVertexReader createVertexReader(InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) {
    return new VariableVertexReader();
  }

  private final class VariableVertexReader extends TextVertexReader {

    @Override
    public boolean nextVertex() throws IOException, InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    @Override
    public Vertex<UserGroup, SendableRiskScores, NullWritable> getCurrentVertex()
        throws IOException, InterruptedException {
      Text line = getRecordReader().getCurrentValue();
      VariableVertex variableVertex = OBJECT_MAPPER
          .readValue(line.toString(), VariableVertex.class);
      Vertex<UserGroup, SendableRiskScores, NullWritable> vertex = getConf().createVertex();
      vertex.initialize(variableVertex.getVertexId(), variableVertex.getVertexValue());
      return vertex;
    }
  }
}
