package algorithm.computation;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import lombok.extern.log4j.Log4j2;
import model.contact.Contact;
import model.identity.UserGroup;
import model.identity.UserId;
import model.score.SendableRiskScores;
import model.score.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
// TODO Benchmark stream and parallel stream

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the
 * elements that comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link UserGroup}</li>
 *     <li>{@link Vertex} data: {@link Contact}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link SendableRiskScores}</li>
 *     <li>Output message: {@link SendableRiskScores}</li>
 * </ul>
 * Each factor vertex receives one or more {@link TemporalUserRiskScore}s from each of its variable vertices. After
 * computation, the factor vertex sends a single {@link TemporalUserRiskScore} to each of its variable vertices.
 */
@Log4j2
public final class FactorVertexComputation
    extends
    AbstractComputation<UserGroup, Contact, NullWritable, SendableRiskScores, SendableRiskScores> {

  private static final double DEFAULT_RISK_SCORE = 0.0;

  private static Collection<SendableRiskScores> withoutInvalidMessages(
      Vertex<UserGroup, Contact, NullWritable> vertex,
      Iterable<SendableRiskScores> iterable) {
    Collection<SendableRiskScores> updatedBeforeLast = new HashSet<>(vertex.getId().size());
    Instant lastTime = getLastTimeOfContact(vertex);
    iterable.forEach(riskScores -> updatedBeforeLast.add(onlyUpdatedBefore(riskScores, lastTime)));
    return updatedBeforeLast;
  }

  private static Instant getLastTimeOfContact(Vertex<UserGroup, Contact, NullWritable> vertex) {
    return vertex.getValue().getOccurrences().first().getTime();
  }

  private static SendableRiskScores onlyUpdatedBefore(SendableRiskScores messages,
      Instant updateCutoff) {
    return messages.withRiskScores(messages.getRiskScores()
        .stream()
        .filter(msg -> msg.getUpdateTime().isAfter(updateCutoff))
        .collect(Collectors.toCollection(TreeSet::new)));
  }

  private static SendableRiskScores finalizeForReceiver(SortedSet<UserId> receiver,
      SendableRiskScores messages) {
    TemporalUserRiskScore toSend;
    if (messages.getRiskScores().isEmpty()) {
      toSend = TemporalUserRiskScore.of(receiver.first(), Instant.now(), DEFAULT_RISK_SCORE);
    } else {
      toSend = Collections.max(messages.getRiskScores());
    }
    return messages.withRiskScores(Set.of(toSend));
  }

  private static boolean areDistinct(Collection<UserId> senders, Collection<UserId> receivers) {
    return !senders.equals(receivers);
  }

  @Override
  public void compute(Vertex<UserGroup, Contact, NullWritable> vertex,
      Iterable<SendableRiskScores> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    sendMessages(vertex.getId(), withoutInvalidMessages(vertex, iterable));
    vertex.voteToHalt();
  }

  private void sendMessages(UserGroup sender, Iterable<SendableRiskScores> messagePool) {
    for (SendableRiskScores riskScores : messagePool) {
      for (SendableRiskScores otherRiskScores : messagePool) {
        if (areDistinct(riskScores.getSenders(), otherRiskScores.getSenders())) {
          sendMessage(sender, finalizeForReceiver(otherRiskScores.getSenders(), riskScores));
        }
      }
    }
  }
}
