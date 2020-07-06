package main.java.algorithm.components;

import main.java.model.ComputedValue;
import main.java.model.RiskScore;
import main.java.model.TemporalUserRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.SortedSet;
import java.util.TreeSet;
// TODO Update Javadoc when types are finalized

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
        // Combine all incoming risk scores with local risk scores
        SortedSet<TemporalUserRiskScore<Long, Double>> incomingRiskScores = new TreeSet<>();
        iterable.forEach(scores -> incomingRiskScores.addAll(scores.getSortedRiskScores()));
        SortedSet<TemporalUserRiskScore<Long, Double>> outgoingRiskScores = new TreeSet<>();
        for (TemporalUserRiskScore<Long, Double> score : incomingRiskScores)
        {
            Instant riskyEncounterTime = score.getUpdateTime();
            Instant mostRecentEncounter = vertex.getValue().getOccurrences().last().getTime();
            // TODO Most recent or earliest?
            if (mostRecentEncounter.isAfter(riskyEncounterTime))
            {
                ComputedValue<Double> noRisk = RiskScore.of(0.0);
                outgoingRiskScores.add(TemporalUserRiskScore.of(score.getUserId(), score.getUpdateTime(), noRisk));
            }
            else
            {
                // TODO Should this be the first contact that comes before this encounter?
                // TODO Duration needs normalization factor
                Duration contactDuration = vertex.getValue().getOccurrences().last().getDuration();
                double updatedRisk = score.getRiskScore().getValue() * (double) contactDuration.toMillis();
                ComputedValue<Double> updated = RiskScore.of(updatedRisk);
                outgoingRiskScores.add(TemporalUserRiskScore.of(score.getUserId(), score.getUpdateTime(), updated));
            }
        }
    }
}
