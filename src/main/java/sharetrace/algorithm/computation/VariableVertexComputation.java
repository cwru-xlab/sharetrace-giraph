package sharetrace.algorithm.computation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
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
    AbstractComputation<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable,
        SendableRiskScoresWritable, SendableRiskScoresWritable> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final String AGGREGATOR_NAME = MasterComputer.getVertexDeltaAggregatorName();

  private UserGroup unwrappedId;

  private SortedSet<RiskScore> localValues;

  private SortedSet<RiskScore> incomingValues;

  private static UserGroupWritableComparable wrapReceiver(RiskScore receivedFromReceiver) {
    return UserGroup.builder()
        .addUser(UserId.of(receivedFromReceiver.getId()))
        .build()
        .wrap();
  }

  @Override
  public void compute(
      Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    unwrap(vertex, iterable);
    aggregate();
    sendMessages();
    vertex.voteToHalt();
  }

  private void unwrap(
      Vertex<UserGroupWritableComparable, SendableRiskScoresWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    unwrappedId = vertex.getId().getUserGroup();
    localValues = vertex.getValue().getSendableRiskScores().getMessage();
    Set<RiskScore> incoming = new HashSet<>();
    iterable.forEach(msg -> incoming.addAll(msg.getSendableRiskScores().getMessage()));
    incomingValues = ImmutableSortedSet.copyOf(incoming);
  }

  private void aggregate() {
    Collection<RiskScore> allValues = new HashSet<>(localValues.size() + incomingValues.size());
    allValues.addAll(localValues);
    allValues.addAll(incomingValues);
    RiskScore prevMax = Collections.max(localValues);
    RiskScore newMax = Collections.max(allValues);
    double delta = Math.abs(newMax.getValue() - prevMax.getValue());
    aggregate(AGGREGATOR_NAME, new DoubleWritable(delta));
  }

  private void sendMessages() {
    incomingValues.parallelStream()
        .forEach(val -> sendMessage(wrapReceiver(val), withoutReceiverMessage(val.getId())));
  }

  private SendableRiskScoresWritable withoutReceiverMessage(String receiver) {
    Collection<RiskScore> withoutReceiverMsg = new HashSet<>(incomingValues);
    withoutReceiverMsg.removeIf(msg -> msg.getId().equals(receiver));
    return SendableRiskScores.builder()
        .setSender(unwrappedId.getUsers())
        .setMessage(withoutReceiverMsg)
        .build()
        .wrap();
  }
}
