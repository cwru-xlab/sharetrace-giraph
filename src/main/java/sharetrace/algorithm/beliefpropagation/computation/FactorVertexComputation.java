package sharetrace.algorithm.beliefpropagation.computation;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
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
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.filter.ExpiredFactorVertexFilter;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.contact.Occurrence;
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
 * Each factor vertex receives {@link SendableRiskScores} from each of its variable vertices. After
 * computation, the factor vertex sends {@link SendableRiskScores} containing a single
 * {@link RiskScore} to each of its variable vertices.
 */
public final class FactorVertexComputation extends
    BasicComputation<UserGroupWritableComparable, ContactWritable, NullWritable, SendableRiskScoresWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final double DEFAULT_RISK_SCORE = 0.0;

  private static final double TRANSMISSION_PROBABILITY = 0.7;

  private static final Instant CUTOFF = ExpiredFactorVertexFilter.getCutoff();

  private static boolean isTransmitted() {
    return TRANSMISSION_PROBABILITY <= ThreadLocalRandom.current().nextDouble(1.0);
  }

  @Override
  public void compute(Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex,
      Iterable<SendableRiskScoresWritable> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);
    if (0 == getSuperstep()) {
      filterExpiredVertexValues(vertex);
    }
    Contact vertexValue = vertex.getValue().getContact();
    Collection<SendableRiskScores> incomingValues = getIncomingValues(iterable);
    Collection<SendableRiskScores> validMessages = retainValidMessages(vertexValue, incomingValues);
    sendMessages(validMessages);
    vertex.voteToHalt();
  }

  // Remove occurrences from the Contact if they occurred before the expiration date.
  private void filterExpiredVertexValues(
      Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex) {
    SortedSet<Occurrence> values = new TreeSet<>(vertex.getValue().getContact().getOccurrences());
    ContactWritable updateValue = Contact.copyOf(vertex.getValue().getContact())
        .withOccurrences(values.stream()
            .filter(occurrence -> occurrence.getTime().isAfter(CUTOFF))
            .collect(Collectors.toSet())).wrap();
    vertex.setValue(updateValue);
  }

  // Add incoming values into a Collection sub-interface for easier handling
  private Collection<SendableRiskScores> getIncomingValues(
      Iterable<SendableRiskScoresWritable> iterable) {
    Set<SendableRiskScores> incoming = new HashSet<>();
    iterable.forEach(msg -> incoming.add(msg.getSendableRiskScores()));
    return ImmutableSet.copyOf(incoming);
  }

  // Combine all filtered messages into a single collection
  private Collection<SendableRiskScores> retainValidMessages(Contact vertexValue,
      Collection<SendableRiskScores> incomingValues) {
    Collection<SendableRiskScores> updatedBeforeLast = new HashSet<>();
    Instant lastTime = vertexValue.getOccurrences().first().getTime();
    incomingValues.parallelStream()
        .forEach(msg -> updatedBeforeLast.add(retainIfUpdatedBefore(msg, lastTime)));
    return ImmutableSet.copyOf(updatedBeforeLast);
  }

  // Filter out messages updated after the provided cutoff time
  private SendableRiskScores retainIfUpdatedBefore(SendableRiskScores messages, Instant cutoff) {
    return SendableRiskScores.copyOf(messages)
        .withMessage(messages.getMessage()
            .parallelStream()
            .filter(msg -> msg.getUpdateTime().isBefore(cutoff))
            .collect(Collectors.toCollection(TreeSet::new)));
  }

  // Relay each variable vertex's message and indicate the message was sent from the factor vertex
  private void sendMessages(Iterable<SendableRiskScores> messages) {
    for (SendableRiskScores senderMessage : messages) {
      for (SendableRiskScores receiverMessage : messages) {
        SortedSet<UserId> sender = senderMessage.getSender();
        if (!sender.equals(receiverMessage.getSender())) {
          sendMessage(wrapReceiver(receiverMessage), wrapMessage(sender, receiverMessage));
        }
      }
    }
  }

  private UserGroupWritableComparable wrapReceiver(SendableRiskScores containsReceiver) {
    return UserGroup.builder()
        .addAllUsers(containsReceiver.getSender())
        .build()
        .wrap();
  }

  private SendableRiskScoresWritable wrapMessage(SortedSet<UserId> sender,
      SendableRiskScores message) {
    RiskScore toSend;
    // Provide a default message if the messages is empty or the risk is not to be transmitted
    if (message.getMessage().isEmpty() || !isTransmitted()) {
      toSend = RiskScore.builder()
          .setId(sender.first().getId())
          .setUpdateTime(MasterComputer.getInitializedAt())
          .setValue(DEFAULT_RISK_SCORE)
          .build();
    } else {
      // Otherwise select the message with the maximum value
      toSend = Collections.max(message.getMessage());
    }
    // Sign the message as being sent from the factor vertex
    return SendableRiskScores.builder()
        .addAllSender(sender)
        .addMessage(toSend)
        .build()
        .wrap();
  }
}
