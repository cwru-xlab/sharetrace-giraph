package main.java.algorithm.components;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.log4j.Log4j2;
import main.java.model.Identifiable;
import main.java.model.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Collection;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the elements that
 * comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link Users}</li>
 *     <li>{@link Vertex} data: {@link RiskScoreData}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link RiskScoreData}</li>
 *     <li>Output message: {@link SortedRiskScores}</li>
 * </ul>
 * Each variable {@link Vertex} receives a single {@link TemporalUserRiskScore} from each of its factor vertices. After
 * computation, the variable {@link Vertex} sends a collection of {@link TemporalUserRiskScore}s to each of its variable
 * vertices.
 */
@Log4j2
@Data
@EqualsAndHashCode(callSuper = true)
public class VariableVertexComputation
        extends AbstractComputation<Users, SortedRiskScores, NullWritable, RiskScoreData, SortedRiskScores>
{
    @Override
    public final void compute(Vertex<Users, SortedRiskScores, NullWritable> vertex, Iterable<RiskScoreData> iterable)
            throws IOException
    {
        NavigableSet<TemporalUserRiskScore<Long, Double>> localRiskScores = vertex.getValue().getSortedRiskScores();
        NavigableSet<TemporalUserRiskScore<Long, Double>> allRiskScores = combine(localRiskScores, iterable);
        for (TemporalUserRiskScore<Long, Double> score : allRiskScores)
        {
            Users receiver = finalizeReceiver(score.getUserId());
            Identifiable<Long> sender = vertex.getId().getUsers().first();
            SortedRiskScores outgoingMessages = finalizeOutgoing(sender, allRiskScores, score);
            sendMessage(receiver, outgoingMessages);
        }
    }

    private static NavigableSet<TemporalUserRiskScore<Long, Double>> combine(
            Collection<TemporalUserRiskScore<Long, Double>> localRiskScores,
            Iterable<RiskScoreData> incomingRiskScores)
    {
        NavigableSet<TemporalUserRiskScore<Long, Double>> allRiskScores = new TreeSet<>();
        incomingRiskScores.forEach(score -> allRiskScores.add(score.getRiskScore()));
        allRiskScores.addAll(localRiskScores);
        return allRiskScores;
    }

    private static SortedRiskScores finalizeOutgoing(
            Identifiable<Long> sender,
            Collection<TemporalUserRiskScore<Long, Double>> allRiskScores,
            TemporalUserRiskScore<Long, Double> receiver)
    {
        NavigableSet<TemporalUserRiskScore<Long, Double>> outgoing = new TreeSet<>(allRiskScores);
        outgoing.remove(receiver);
        return SortedRiskScores.of(sender, outgoing);
    }

    private static Users finalizeReceiver(Identifiable<Long> receiverId)
    {
        NavigableSet<Identifiable<Long>> receiver = new TreeSet<>();
        receiver.add(receiverId);
        return Users.of(receiver);
    }
}
