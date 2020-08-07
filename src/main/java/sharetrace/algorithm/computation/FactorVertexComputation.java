package sharetrace.algorithm.computation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.ContactWritable;
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
 *     <li>{@link Vertex} data: {@link Contact}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link SendableRiskScores}</li>
 *     <li>Output message: {@link SendableRiskScores}</li>
 * </ul>
 * Each factor vertex receives one or more {@link RiskScore}s from each of its variable vertices. After
 * computation, the factor vertex sends a single {@link RiskScore} to each of its variable vertices.
 */
public final class FactorVertexComputation extends
    AbstractComputation<UserGroupWritableComparable, ContactWritable, NullWritable,
        SendableRiskScoresWritable, SendableRiskScoresWritable> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final double DEFAULT_RISK_SCORE = 0.0;

  private static final double TRANSMISSION_PROBABILITY = 0.7;

  private UserGroupWritableComparable wrappedId;

  private UserGroup unwrappedId;

  private Contact vertexValue;

  private Iterable<SendableRiskScores> incomingMessages;

  // Filter out messages updated after the provided cutoff time
  private static SendableRiskScores onlyUpdatedBefore(SendableRiskScores messages,
      Instant cutoff) {
    return SendableRiskScores.copyOf(messages)
        .withMessage(messages.getMessage()
            .stream()
            .filter(msg -> msg.getUpdateTime().isBefore(cutoff))
            .collect(Collectors.toCollection(TreeSet::new)));
  }

  private static boolean isTransmitted() {
    return TRANSMISSION_PROBABILITY <= ThreadLocalRandom.current().nextDouble(1.0);
  }

  @Override
  public void compute(Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    unwrap(vertex, iterable);
    sendMessages(withoutInvalidMessages());
    vertex.voteToHalt();
  }

  // Makes the helper methods a little cleaner
  private void unwrap(
      Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    // Used to send messages
    wrappedId = vertex.getId();
    unwrappedId = wrappedId.getUserGroup();
    vertexValue = vertex.getValue().getContact();
    // Combine incoming messages
    Set<SendableRiskScores> incoming = new HashSet<>();
    iterable.forEach(msg -> incoming.add(msg.getSendableRiskScores()));
    incomingMessages = ImmutableSortedSet.copyOf(incoming);
  }

  // Combine all filtered messages into a single collection
  private Collection<SendableRiskScores> withoutInvalidMessages() {
    Collection<SendableRiskScores> updatedBeforeLast = new HashSet<>();
    Instant lastTime = vertexValue.getOccurrences().first().getTime();
    incomingMessages.forEach(msg -> updatedBeforeLast.add(onlyUpdatedBefore(msg, lastTime)));
    return ImmutableSet.copyOf(updatedBeforeLast);
  }

  // Relay each variable vertex's message and indicate the message was sent from the factor vertex
  private void sendMessages(Iterable<SendableRiskScores> messagePool) {
    for (SendableRiskScores riskScores : messagePool) {
      for (SendableRiskScores otherRiskScores : messagePool) {
        if (!riskScores.getSender().equals(otherRiskScores.getSender())) {
          sendMessage(wrappedId, finalizeForReceiver(riskScores.getSender(), otherRiskScores));
        }
      }
    }
  }

  private SendableRiskScoresWritable finalizeForReceiver(SortedSet<UserId> receiver,
      SendableRiskScores messages) {
    RiskScore toSend;
    // Provide a default message if the messages is empty or the risk is not to be transmitted
    if (messages.getMessage().isEmpty() || !isTransmitted()) {
      toSend = RiskScore.builder()
          .setId(receiver.first().getId())
          .setUpdateTime(Instant.now())
          .setValue(DEFAULT_RISK_SCORE)
          .build();
    } else {
      // Otherwise select the message with the maximum value
      toSend = Collections.max(messages.getMessage());
    }
    // Sign the message as being sent from the factor vertex
    return SendableRiskScores.builder()
        .setSender(unwrappedId.getUsers())
        .addMessage(toSend)
        .build()
        .wrap();
  }
}
