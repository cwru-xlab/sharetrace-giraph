package main.java.algorithm.components;

import main.java.model.Identifiable;
import main.java.model.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
// TODO Update Javadoc when types are finalized

/**
 * Computation performed at every factor {@link Vertex} of the factor graph. The following are the elements that
 * comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link Users}</li>
 *     <li>{@link Vertex} data: {@link RiskScoreData}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link SortedRiskScores}</li>
 *     <li>Output message: {@link SortedRiskScores}</li>
 * </ul>
 * Each variable {@link Vertex} receives a single {@link TemporalUserRiskScore} from each of its factor vertices. After
 * computation, the variable {@link Vertex} sends a collection of {@link TemporalUserRiskScore}s to each of its variable
 * vertices.
 */
public class VariableVertexComputation
        extends AbstractComputation<Users, SortedRiskScores, NullWritable, SortedRiskScores, SortedRiskScores>
{
    // TODO Assumes all risk scores are kept in the vertex
    // TODO Iterable<SortedRiskScores> might actually need to be Iterable<RiskScoreData>
    @Override
    public void compute(Vertex<Users, SortedRiskScores, NullWritable> vertex, Iterable<SortedRiskScores> iterable)
            throws IOException
    {
        // Combine all incoming risk scores with local risk scores
        SortedSet<TemporalUserRiskScore<Long, Double>> allRiskScores = new TreeSet<>();
        iterable.forEach(scores -> allRiskScores.addAll(scores.getSortedRiskScores()));
        allRiskScores.addAll(vertex.getValue().getSortedRiskScores());
        // Send all incoming risk scores to each factor vertex, except the one that came from that vertex
        for (TemporalUserRiskScore<Long, Double> score : allRiskScores)
        {
            SortedSet<TemporalUserRiskScore<Long, Double>> outgoingRiskScores = new TreeSet<>(allRiskScores);
            outgoingRiskScores.remove(score);
            Set<Identifiable<Long>> user = Set.of(score.getUserId());
            sendMessage(Users.of(user), SortedRiskScores.of(outgoingRiskScores));
        }
    }
}
