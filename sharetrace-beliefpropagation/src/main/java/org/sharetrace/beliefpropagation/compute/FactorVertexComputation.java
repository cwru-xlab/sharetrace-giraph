package org.sharetrace.beliefpropagation.compute;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.format.writable.VariableVertexValue;
import org.sharetrace.beliefpropagation.param.BPContext;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
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
    BasicComputation<FactorGraphVertexId, FactorGraphWritable, NullWritable, VariableVertexValue> {

  // Logging messages
  private static final String NULL_VERTEX_MSG = "Vertex must not be null";
  private static final String NULL_MESSAGE_MSG = "Messages must not be null";
  private static final String HALT_MSG = "Halting computation: vertex is not a factor vertex";
  private static final String FILTER_MSG = "Filtering expired vertex values on zeroth superstep";
  private static final String REMOVE_EXPIRED_MSG = "Removing expired vertex values...";
  private static final String RETAIN_MSG = "Retaining valid messages...";
  private static final String NO_OCCURRENCES_MSG =
      "No messages to retain since the vertex had no occurrences";
  private static final String SEND_MSG = "Sending messages...";
  private static final String SEND_PATTERN = "Sending message from {0} to {1}";

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final double DEFAULT_RISK_SCORE = BPContext.getDefaultRiskScore();

  private static final double TRANSMISSION_PROBABILITY = BPContext.getTransmissionProbability();

  private static final Instant CUTOFF = BPContext.getOccurrenceLookbackCutoff();

  @Override
  public void compute(Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex,
      Iterable<VariableVertexValue> iterable) {
    Preconditions.checkNotNull(vertex, NULL_VERTEX_MSG);
    Preconditions.checkNotNull(iterable, NULL_MESSAGE_MSG);

    if (vertex.getValue().getType().equals(VertexType.VARIABLE)) {
      LOGGER.debug(HALT_MSG);
      vertex.voteToHalt();
      return;
    }

    if (0 == getSuperstep()) {
      LOGGER.debug(FILTER_MSG);
      updateVertexValue(vertex);
    }

    Contact vertexValue = ((FactorVertexValue) vertex.getValue().getWrapped()).getValue();
    Collection<SendableRiskScores> incomingValues = getIncomingValues(iterable);
    Collection<SendableRiskScores> validMessages = retainValidMessages(vertexValue, incomingValues);
    sendMessages(validMessages);
    vertex.voteToHalt();
  }

  // Remove occurrences from the Contact if they occurred before the expiration date.
  private void updateVertexValue(
      Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex) {
    LOGGER.debug(REMOVE_EXPIRED_MSG);
    Contact value = ((FactorVertexValue) vertex.getValue().getWrapped()).getValue();
    vertex.setValue(FactorGraphWritable.ofFactorVertex(removedExpiredValues(value)));
  }

  @VisibleForTesting
  FactorVertexValue removedExpiredValues(Contact value) {
    Set<Occurrence> withoutExpiredValues = value.getOccurrences().stream()
        .filter(occurrence -> occurrence.getTime().isAfter(getCutoff()))
        .collect(Collectors.toSet());
    return FactorVertexValue.of(Contact.copyOf(value).withOccurrences(withoutExpiredValues));
  }

  // Add incoming values into a Collection sub-interface for easier handling
  @VisibleForTesting
  Set<SendableRiskScores> getIncomingValues(Iterable<VariableVertexValue> iterable) {
    Set<SendableRiskScores> incoming = new HashSet<>();
    iterable.forEach(msg -> incoming.add(msg.getValue()));
    return ImmutableSet.copyOf(incoming);
  }

  // Combine all filtered messages into a single collection
  @VisibleForTesting
  Set<SendableRiskScores> retainValidMessages(Contact vertexValue,
      Collection<SendableRiskScores> incomingValues) {
    LOGGER.debug(RETAIN_MSG);
    SortedSet<Occurrence> occurrences = vertexValue.getOccurrences();
    Set<SendableRiskScores> retained = new HashSet<>();
    if (occurrences.isEmpty()) {
      LOGGER.debug(NO_OCCURRENCES_MSG);
    } else {
      // Get the most recent time that the two users came into contact
      Instant lastTime = vertexValue.getOccurrences().last().getTime();
      // Retain only the scores that were updated before this point in time
      retained = Stream.of(incomingValues)
          .flatMap(Collection::stream)
          .map(msg -> retainIfUpdatedBefore(msg, lastTime))
          .filter(msg -> !msg.getMessage().isEmpty())
          .collect(Collectors.toSet());
    }
    return ImmutableSet.copyOf(retained);
  }

  // Filter out messages updated after the provided cutoff time
  @VisibleForTesting
  SendableRiskScores retainIfUpdatedBefore(SendableRiskScores messages, Instant cutoff) {
    return SendableRiskScores.copyOf(messages)
        .withMessage(messages.getMessage()
            .stream()
            .filter(msg -> msg.getUpdateTime().isBefore(cutoff))
            .collect(Collectors.toCollection(TreeSet::new)));
  }

  // Relay each variable vertex's message and indicate the message was sent from the factor vertex
  private void sendMessages(Iterable<SendableRiskScores> messages) {
    LOGGER.debug(SEND_MSG);
    for (SendableRiskScores senderMessage : messages) {
      for (SendableRiskScores receiverMessage : messages) {
        SortedSet<String> sender = senderMessage.getSender();
        if (!sender.equals(receiverMessage.getSender())) {
          LOGGER.debug(MessageFormat.format(SEND_PATTERN, sender, receiverMessage.getSender()));
          sendMessage(wrapReceiver(receiverMessage), wrapMessage(sender, receiverMessage));
        }
      }
    }
  }

  @VisibleForTesting
  FactorGraphVertexId wrapReceiver(SendableRiskScores containsReceiver) {
    return FactorGraphVertexId
        .of(IdGroup.builder().addAllIds(containsReceiver.getSender()).build());
  }

  @VisibleForTesting
  VariableVertexValue wrapMessage(SortedSet<String> sender, SendableRiskScores message) {
    RiskScore toSend;
    // Provide a default message if the messages is empty or the risk is not to be transmitted
    if (message.getMessage().isEmpty() || !isTransmitted()) {
      toSend = RiskScore.builder()
          .id(sender.first())
          .updateTime(getInitializedAt())
          .value(DEFAULT_RISK_SCORE)
          .build();
    } else {
      // Otherwise select the message with the maximum value
      toSend = Collections.max(message.getMessage());
    }
    // Sign the message as being sent from the factor vertex
    return VariableVertexValue.of(SendableRiskScores.builder()
        .addAllSender(sender)
        .addMessage(toSend)
        .build());
  }

  @VisibleForTesting
  Instant getCutoff() {
    return CUTOFF;
  }

  @VisibleForTesting
  boolean isTransmitted() {
    return TRANSMISSION_PROBABILITY <= ThreadLocalRandom.current().nextDouble(1.0);
  }

  @VisibleForTesting
  Instant getInitializedAt() {
    return MasterComputer.getInitializedAt();
  }
}
