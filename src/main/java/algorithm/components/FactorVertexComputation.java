package main.java.algorithm.components;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;
import main.java.model.Identifiable;
import main.java.model.RiskScore;
import main.java.model.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the elements that
 * comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link Users}</li>
 *     <li>{@link Vertex} data: {@link ContactData}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link SortedRiskScores}</li>
 *     <li>Output message: {@link RiskScoreData}</li>
 * </ul>
 * Each factor vertex receives one or more {@link TemporalUserRiskScore}s from each of its variable vertices. After
 * computation, the factor vertex sends a single {@link TemporalUserRiskScore} to each of its variable vertices.
 */
@Log4j2
@Data
@EqualsAndHashCode(callSuper = true)
public class FactorVertexComputation
        extends AbstractComputation<Users, ContactData, NullWritable, SortedRiskScores, RiskScoreData>
{
    @Override
    public final void compute(Vertex<Users, ContactData, NullWritable> vertex, Iterable<SortedRiskScores> iterable)
            throws IOException
    {
        // Get all receiver-scores pairs, where the scores were not sent from the receiver
        GetRiskScoresAndReceivers<Long> pairs = new GetRiskScoresAndReceivers<>(vertex.getId().getUsers(), iterable);
        Instant earliestTime = getEarliestTimeOfContact(vertex.getValue());
        pairs.getEntries()
             .parallelStream()
             .forEach(e -> sendMessage(finalizeReceiver(e.getKey()),
                                       finalizeMessage(e.getValue().filterOutBefore(earliestTime), e.getKey())));
    }

    private static Instant getEarliestTimeOfContact(@NonNull ContactData data)
    {
        return data.getOccurrences().first().getTime();
    }

    private static RiskScoreData finalizeMessage(
            @NonNull Collection<TemporalUserRiskScore<Long, Double>> scores,
            @NonNull Identifiable<Long> receiverId)
    {
        TemporalUserRiskScore<Long, Double> outgoing;
        if (scores.isEmpty())
        {
            outgoing = TemporalUserRiskScore.of(receiverId, Instant.now(), RiskScore.of(0.0));
        }
        else
        {
            outgoing = Collections.max(scores, Comparator.comparing(TemporalUserRiskScore::getRiskScore));
        }
        return RiskScoreData.of(outgoing);
    }

    private static Users finalizeReceiver(@NonNull Identifiable<Long> receiverId)
    {
        NavigableSet<Identifiable<Long>> receiver = new TreeSet<>();
        receiver.add(receiverId);
        return Users.of(receiver);
    }

    private static final class GetRiskScoresAndReceivers<T>
    {
        private final Set<Map.Entry<Identifiable<T>, SortedRiskScores>> outgoingMessages;

        private GetRiskScoresAndReceivers(Collection<? extends Identifiable<T>> users,
                                          Iterable<SortedRiskScores> riskScores)
        {
            Collection<Map.Entry<Identifiable<T>, SortedRiskScores>> messages = new HashSet<>(users.size());
            users.forEach(u -> riskScores.forEach(r -> messages.add(new AbstractMap.SimpleImmutableEntry<>(u, r))));
            outgoingMessages = messages.parallelStream()
                                       .filter(Predicate.not(msg -> msg.getKey().equals(msg.getValue().getSender())))
                                       .collect(Collectors.toUnmodifiableSet());
        }

        private Set<Map.Entry<Identifiable<T>, SortedRiskScores>> getEntries()
        {
            return outgoingMessages;
        }

        @Override
        public String toString()
        {
            Object[] args = {getClass().getName(), "outgoingMessages", outgoingMessages.toString()};
            return MessageFormat.format("{1}({2}={3})", args);
        }
    }
}
