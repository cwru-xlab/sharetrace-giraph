package org.sharetrace.beliefpropagation.compute;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.model.identity.IdGroup;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the
 * elements that comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link IdGroup}</li>
 *     <li>{@link Vertex} data: {@link SendableRiskScores}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link SendableRiskScores}</li>
 *     <li>Output message: {@link SendableRiskScores}</li>
 * </ul>
 * Each variable {@link Vertex} receives a single {@link RiskScore} from each of its factor vertices.
 * After
 * computation, the variable {@link Vertex} sends a collection of {@link RiskScore}s to each of its
 * variable
 * vertices.
 */
public final class VariableVertexComputation extends
    BasicComputation<FactorGraphVertexId, FactorGraphWritable, NullWritable, VariableVertexValue> {

  // Logging messages
  private static final String HALTING_MSG = "Halting computation: vertex is not a variable vertex";
  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final String AGGREGATOR_NAME = MasterComputer.getVertexDeltaAggregatorName();

  private static final Comparator<? super RiskScore> COMPARE_BY_RISK_SCORE =
      Comparator.comparing(RiskScore::getValue);

  @Override
  public void compute(Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex,
      Iterable<VariableVertexValue> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);

    if (vertex.getValue().getType().equals(VertexType.FACTOR)) {
      LOGGER.debug(HALTING_MSG);
      vertex.voteToHalt();
      return;
    }

    SendableRiskScores value = ((VariableVertexValue) vertex.getValue().getWrapped()).getValue();
    Collection<RiskScore> localValues = value.getMessage();
    Collection<RiskScore> incomingValues = getIncomingValues(iterable);
    Collection<RiskScore> allValues = combineValues(localValues, incomingValues);
    Collection<String> vertexId = vertex.getId().getIdGroup().getIds();
    updateVertexValue(vertex, vertexId, allValues);
    aggregate(localValues, incomingValues);
    sendMessages(vertexId, allValues);
    vertex.voteToHalt();
  }

  @VisibleForTesting
  SortedSet<RiskScore> getIncomingValues(Iterable<VariableVertexValue> iterable) {
    Collection<RiskScore> incoming = new TreeSet<>();
    iterable.forEach(msg -> incoming.addAll(msg.getValue().getMessage()));
    return ImmutableSortedSet.copyOf(incoming);
  }

  @VisibleForTesting
  SortedSet<RiskScore> combineValues(Collection<RiskScore> values,
      Collection<RiskScore> otherValues) {
    return ImmutableSortedSet.copyOf(Stream.of(values, otherValues)
        .flatMap(Collection::stream)
        .collect(Collectors.toSet()));
  }

  private void updateVertexValue(
      Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex,
      Collection<String> valueId, Collection<RiskScore> newValues) {
    vertex.setValue(FactorGraphWritable.ofVariableVertex(getUpdatedValue(valueId, newValues)));
  }

  @VisibleForTesting
  VariableVertexValue getUpdatedValue(Collection<String> valueId, Collection<RiskScore> newValues) {
    return VariableVertexValue.of(SendableRiskScores.builder()
        .addAllMessage(newValues)
        .sender(valueId)
        .build());
  }

  private void aggregate(Collection<RiskScore> values, Collection<RiskScore> otherValues) {
    aggregate(AGGREGATOR_NAME, new DoubleWritable(getMaxValueDelta(values, otherValues)));
  }

  @VisibleForTesting
  double getMaxValueDelta(Collection<RiskScore> values, Collection<RiskScore> otherValues) {
    double localMax = Collections.max(values, COMPARE_BY_RISK_SCORE).getValue();
    double incomingMax = Collections.max(otherValues, COMPARE_BY_RISK_SCORE).getValue();
    return Math.abs(incomingMax - localMax);
  }

  private void sendMessages(Collection<String> sender, Collection<RiskScore> messages) {
    messages.forEach(msg -> sendMessage(wrapReceiver(msg), wrapMessage(sender, msg, messages)));
  }

  @VisibleForTesting
  FactorGraphVertexId wrapReceiver(RiskScore value) {
    return FactorGraphVertexId.of(IdGroup.builder().addId(value.getId()).build());
  }

  @VisibleForTesting
  VariableVertexValue wrapMessage(Collection<String> sender, RiskScore fromReceiver,
      Collection<RiskScore> messages) {
    return VariableVertexValue.of(SendableRiskScores.builder()
        .sender(sender)
        .message(messages.stream()
            .filter(msg -> !msg.getId().equals(fromReceiver.getId()))
            .collect(Collectors.toSet()))
        .build());
  }
}
