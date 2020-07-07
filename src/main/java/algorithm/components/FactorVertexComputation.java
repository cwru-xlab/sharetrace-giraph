package main.java.algorithm.components;

import lombok.NonNull;
import main.java.model.Identifiable;
import main.java.model.RiskScore;
import main.java.model.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
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
public class FactorVertexComputation
        extends AbstractComputation<Users, ContactData, NullWritable, SortedRiskScores, RiskScoreData>
{
    @Override
    public void compute(Vertex<Users, ContactData, NullWritable> vertex, Iterable<SortedRiskScores> iterable)
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

    private static RiskScoreData finalizeOutgoingMessage(
            @NonNull Collection<TemporalUserRiskScore<Long, Double>> remaining,
            @NonNull Identifiable<Long> receiverId)
    {
        TemporalUserRiskScore<Long, Double> outgoingMessage;
        if (remaining.isEmpty())
        {
            outgoingMessage = TemporalUserRiskScore.of(receiverId, Instant.now(), RiskScore.of(0.0));
        }
        else
        {
            outgoingMessage = Collections.max(remaining, Comparator.comparing(TemporalUserRiskScore::getRiskScore));
        }
        return RiskScoreData.of(outgoingMessage);
    }

    private static Users finalizeReceiver(@NonNull Identifiable<Long> receiverId)
    {
        NavigableSet<Identifiable<Long>> receiver = new TreeSet<>();
        receiver.add(receiverId);
        return Users.of(receiver);
    }

    private class GetRiskScoresAndRecivers<T>
    {
        private final Set<Map.Entry<Identifiable<T>, SortedRiskScores>> outgoingMessages;

        private GetRiskScoresAndRecivers(Collection<? extends Identifiable<T>> users,
                                         Iterable<? extends SortedRiskScores> riskScores)
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
    }
}
