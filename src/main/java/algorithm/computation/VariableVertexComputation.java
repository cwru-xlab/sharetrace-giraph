package algorithm.computation;

import com.google.common.base.Preconditions;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;
import model.identity.UserGroup;
import model.identity.UserId;
import model.score.SendableRiskScores;
import model.score.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 * Each variable {@link Vertex} receives a single {@link TemporalUserRiskScore} from each of its factor vertices. After
 * computation, the variable {@link Vertex} sends a collection of {@link TemporalUserRiskScore}s to each of its variable
 * vertices.
 */
public final class VariableVertexComputation
    extends
    AbstractComputation<UserGroup, SendableRiskScores, NullWritable, SendableRiskScores, SendableRiskScores> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final String AGGREGATOR_NAME = MasterComputer.getVertexDeltaAggregatorName();

  private static SortedSet<TemporalUserRiskScore> getLocal(
      Vertex<UserGroup, SendableRiskScores, NullWritable> vertex) {
    return vertex.getValue().getRiskScores();
  }

  private static SortedSet<TemporalUserRiskScore> getIncoming(
      Iterable<SendableRiskScores> incomingMessages) {
    SortedSet<TemporalUserRiskScore> incoming = new TreeSet<>();
    incomingMessages.forEach(msg -> incoming.addAll(msg.getRiskScores()));
    return incoming;
  }

  private static SortedSet<TemporalUserRiskScore> combine(Collection<TemporalUserRiskScore> values,
      Collection<TemporalUserRiskScore> otherValues) {
    SortedSet<TemporalUserRiskScore> all = new TreeSet<>(values);
    all.addAll(otherValues);
    return all;
  }

  private static SendableRiskScores withoutReceiverMsg(UserGroup sender,
      UserId receiver,
      Collection<TemporalUserRiskScore> allMessages) {
    Collection<TemporalUserRiskScore> withoutReceiverMsg = new HashSet<>(allMessages);
    withoutReceiverMsg.removeIf(msg -> receiver.equals(msg.getUserId()));
    return SendableRiskScores.of(sender, withoutReceiverMsg);
  }

  @Override
  public void compute(Vertex<UserGroup, SendableRiskScores, NullWritable> vertex,
      Iterable<SendableRiskScores> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    SortedSet<TemporalUserRiskScore> local = getLocal(vertex);
    SortedSet<TemporalUserRiskScore> incoming = getIncoming(iterable);
    aggregate(local, combine(local, incoming));
    sendMessages(vertex.getId(), incoming);
    vertex.voteToHalt();
  }

  private void aggregate(Collection<TemporalUserRiskScore> local,
      Collection<TemporalUserRiskScore> all) {
    TemporalUserRiskScore prevMax = Collections.max(local);
    TemporalUserRiskScore newMax = Collections.max(all);
    double delta = Math.abs(newMax.getRiskScore() - prevMax.getRiskScore());
    aggregate(AGGREGATOR_NAME, new DoubleWritable(delta));
  }

  private void sendMessages(UserGroup sender, Collection<TemporalUserRiskScore> toSend) {
    toSend.parallelStream()
        .forEach(val -> sendMessage(UserGroup.of(val.getUserId()),
            withoutReceiverMsg(sender, val.getUserId(), toSend)));
  }
}
