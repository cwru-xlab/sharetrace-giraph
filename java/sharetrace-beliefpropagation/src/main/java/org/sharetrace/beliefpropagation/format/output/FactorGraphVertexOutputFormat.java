package org.sharetrace.beliefpropagation.format.output;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FactorGraphVertexOutputFormat extends
    TextVertexOutputFormat<FactorGraphVertexId, FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexOutputFormat.class);

  private static final Comparator<? super RiskScore> COMPARE_BY_RISK_TIME_ID =
      Comparator.comparing(RiskScore::getValue)
          .thenComparing(RiskScore::getUpdateTime)
          .thenComparing(RiskScore::getId);

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

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
      Text text = new Text();
      if (writable.getType().equals(VertexType.VARIABLE)) {
        SendableRiskScores value = ((VariableVertexValue) writable.getWrapped()).getValue();
        SortedSet<RiskScore> orderedByRiskScore = new TreeSet<>(COMPARE_BY_RISK_TIME_ID);
        orderedByRiskScore.addAll(value.getMessage());
        String valueAsString = MAPPER.writeValueAsString(orderedByRiskScore.last());
        text = new Text(valueAsString);
      }
      return text;
    }
  }
}
