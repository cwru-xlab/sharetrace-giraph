package sharetrace.algorithm.beliefpropagation.computation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.identity.UserGroupWritableComparable;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;
import sharetrace.model.score.SendableRiskScoresWritable;

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the
 * elements that comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link UserGroup}</li>
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
    BasicComputation<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable, SendableRiskScoresWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final String AGGREGATOR_NAME = MasterComputer.getVertexDeltaAggregatorName();

  private static final Comparator<? super RiskScore> COMPARE_BY_RISK_SCORE =
      Comparator.comparing(RiskScore::getValue);

  @Override
  public void compute(
      Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    Collection<RiskScore> localValues = vertex.getValue().getSendableRiskScores().getMessage();
    Collection<RiskScore> incomingValues = getIncomingValues(iterable);
    Collection<RiskScore> allValues = combineValues(localValues, incomingValues);
    Collection<UserId> vertexId = vertex.getId().getUserGroup().getUsers();
    updateVertexValue(vertex, vertexId, allValues);
    aggregate(localValues, incomingValues);
    sendMessages(vertexId, allValues);
    vertex.voteToHalt();
  }

  private Collection<RiskScore> getIncomingValues(Iterable<SendableRiskScoresWritable> iterable) {
    SortedSet<RiskScore> incoming = new TreeSet<>();
    iterable.forEach(msg -> incoming.addAll(msg.getSendableRiskScores().getMessage()));
    return ImmutableSortedSet.copyOf(incoming);
  }

  private Collection<RiskScore> combineValues(Collection<RiskScore> values,
      Collection<RiskScore> otherValues) {
    SortedSet<RiskScore> combined = new TreeSet<>();
    combined.addAll(values);
    combined.addAll(otherValues);
    return ImmutableSortedSet.copyOf(combined);
  }

  private void updateVertexValue(
      Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex,
      Collection<UserId> valueId, Collection<RiskScore> newValues) {
    vertex.setValue(SendableRiskScores.builder()
        .addAllMessage(newValues)
        .setSender(valueId)
        .build()
        .wrap());
  }

  private void aggregate(Collection<RiskScore> values, Collection<RiskScore> otherValues) {
    double localMax = Collections.max(values, COMPARE_BY_RISK_SCORE).getValue();
    double incomingMax = Collections.max(otherValues, COMPARE_BY_RISK_SCORE).getValue();
    aggregate(AGGREGATOR_NAME, new DoubleWritable(Math.abs(incomingMax - localMax)));
  }

  private void sendMessages(Collection<UserId> sender, Collection<RiskScore> messages) {
    messages.parallelStream()
        .forEach(msg -> sendMessage(wrapReceiver(msg), wrapMessage(sender, msg, messages)));
  }

  private UserGroupWritableComparable wrapReceiver(RiskScore value) {
    return UserGroup.builder()
        .addUser(UserId.of(value.getId()))
        .build()
        .wrap();
  }

  private SendableRiskScoresWritable wrapMessage(Collection<UserId> sender, RiskScore fromReceiver,
      Collection<RiskScore> messages) {
    Collection<RiskScore> withoutReceiverMsg = new HashSet<>(messages);
    withoutReceiverMsg.removeIf(msg -> msg.getId().equals(fromReceiver.getId()));
    return SendableRiskScores.builder()
        .setSender(sender)
        .setMessage(withoutReceiverMsg)
        .build()
        .wrap();
  }
}
