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
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.*;

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
        GetRiskScoresAndRecivers<Long> pairs = new GetRiskScoresAndRecivers<>(vertex.getId().getUsers(), iterable);
        for (Map.Entry<Identifiable<Long>, SortedRiskScores> pair : pairs.getEntries())
        {
            Identifiable<Long> receiverId = pair.getKey();
            SortedRiskScores scores = pair.getValue();
            Instant earliestTime = getEarliestTimeOfContact(vertex.getValue());
            NavigableSet<TemporalUserRiskScore<Long, Double>> filtered = scores.filterOutBefore(earliestTime);
            sendMessage(finalizeReceiver(receiverId), finalizeOutgoingMessage(filtered, receiverId));
        }
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
            Set<Map.Entry<Identifiable<T>, SortedRiskScores>> messages = new HashSet<>(users.size());
            for (Identifiable<T> receiver : users)
            {
                for (SortedRiskScores scores : riskScores)
                {
                    if (scores.getSender() != receiver)
                    {
                        messages.add(new SimpleImmutableEntry<>(receiver, scores));
                    }
                }
            }
            outgoingMessages = Set.copyOf(messages);
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
