package org.sharetrace.beliefpropagation.computation;

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

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexComputation.class);

  private static final double DEFAULT_RISK_SCORE = BPContext.getDefaultRiskScore();

  private static final double TRANSMISSION_PROBABILITY = BPContext.getTransmissionProbability();

  private static final Instant CUTOFF = BPContext.getOccurrenceLookbackCutoff();

  private static boolean isTransmitted() {
    return TRANSMISSION_PROBABILITY <= ThreadLocalRandom.current().nextDouble(1.0);
  }

  @Override
  public void compute(Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex,
      Iterable<VariableVertexValue> iterable) {
    Preconditions.checkNotNull(vertex);
    Preconditions.checkNotNull(iterable);

    if (vertex.getValue().getType().equals(VertexType.VARIABLE)) {
      vertex.voteToHalt();
      return;
    }

    if (0 == getSuperstep()) {
      filterExpiredVertexValues(vertex);
    }

    Contact vertexValue = ((FactorVertexValue) vertex.getValue().getWrapped()).getContact();
    Collection<SendableRiskScores> incomingValues = getIncomingValues(iterable);
    Collection<SendableRiskScores> validMessages = retainValidMessages(vertexValue, incomingValues);
    sendMessages(validMessages);
    vertex.voteToHalt();
  }

  // Remove occurrences from the Contact if they occurred before the expiration date.
  private void filterExpiredVertexValues(
      Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex) {
    Contact value = ((FactorVertexValue) vertex.getValue().getWrapped()).getContact();
    SortedSet<Occurrence> values = new TreeSet<>(value.getOccurrences());
    FactorVertexValue updateValue = FactorVertexValue.of(Contact.copyOf(value)
        .withOccurrences(values.stream()
            .filter(occurrence -> occurrence.getTime().isAfter(CUTOFF))
            .collect(Collectors.toSet())));
    vertex.setValue(FactorGraphWritable.ofFactorVertex(updateValue));
  }

  // Add incoming values into a Collection sub-interface for easier handling
  private Collection<SendableRiskScores> getIncomingValues(
      Iterable<VariableVertexValue> iterable) {
    Set<SendableRiskScores> incoming = new HashSet<>();
    iterable.forEach(msg -> incoming.add(msg.getSendableRiskScores()));
    return ImmutableSet.copyOf(incoming);
  }

  // Combine all filtered messages into a single collection
  private Collection<SendableRiskScores> retainValidMessages(Contact vertexValue,
      Collection<SendableRiskScores> incomingValues) {
    Collection<SendableRiskScores> updatedBeforeLast = new HashSet<>();
    // If there are no occurrences return an empty set
    Instant lastTime = vertexValue.getOccurrences().first().getTime();
    incomingValues.forEach(msg -> updatedBeforeLast.add(retainIfUpdatedBefore(msg, lastTime)));
    return ImmutableSet.copyOf(updatedBeforeLast);
  }

  // Filter out messages updated after the provided cutoff time
  private SendableRiskScores retainIfUpdatedBefore(SendableRiskScores messages, Instant cutoff) {
    return SendableRiskScores.copyOf(messages)
        .withMessage(messages.getMessage()
            .stream()
            .filter(msg -> msg.getUpdateTime().isBefore(cutoff))
            .collect(Collectors.toCollection(TreeSet::new)));
  }

  // Relay each variable vertex's message and indicate the message was sent from the factor vertex
  private void sendMessages(Iterable<SendableRiskScores> messages) {
    for (SendableRiskScores senderMessage : messages) {
      for (SendableRiskScores receiverMessage : messages) {
        SortedSet<String> sender = senderMessage.getSender();
        if (!sender.equals(receiverMessage.getSender())) {
          sendMessage(wrapReceiver(receiverMessage), wrapMessage(sender, receiverMessage));
        }
      }
    }
  }

  private FactorGraphVertexId wrapReceiver(SendableRiskScores containsReceiver) {
    return FactorGraphVertexId.of(IdGroup.builder()
        .addAllIds(containsReceiver.getSender())
        .build());
  }

  private VariableVertexValue wrapMessage(SortedSet<String> sender,
      SendableRiskScores message) {
    RiskScore toSend;
    // Provide a default message if the messages is empty or the risk is not to be transmitted
    if (message.getMessage().isEmpty() || !isTransmitted()) {
      toSend = RiskScore.builder()
          .setId(sender.first())
          .setUpdateTime(MasterComputer.getInitializedAt())
          .setValue(DEFAULT_RISK_SCORE)
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
}
