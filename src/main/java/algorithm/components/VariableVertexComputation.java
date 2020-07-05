package main.java.algorithm.components;

import main.java.model.TemporalRiskScore;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.ArrayWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * Computation performed at every factor {@link Vertex} of the belief propagation graph. The following are the
 * elements that comprise the computation:
 * <ul>
 *     <li>{@link Vertex} ID: {@link UserIdWritableComparable}</li>
 *     <li>{@link Vertex} data: {@link TemporalRiskScoreWritable}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link TemporalRiskScoreWritable}</li>
 *     <li>Output message: {@link ArrayWritable<TemporalRiskScoreWritable>}</li>
 * </ul>
 * Each variable vertex receives a single {@link TemporalRiskScore}s from each of its factor vertices. After
 * computation, the variable vertex sends a collection of {@link TemporalRiskScore}s to each of its factor vertices.
 */
public class VariableVertexComputation
        extends AbstractComputation<UserIdWritableComparable, TemporalRiskScoreWritable, NullWritable,
        TemporalRiskScoreWritable, ArrayWritable<TemporalRiskScoreWritable>>
{
    @Override
    public void compute(Vertex<UserIdWritableComparable, TemporalRiskScoreWritable, NullWritable> vertex,
                        Iterable<TemporalRiskScoreWritable> iterable) throws IOException
    {
    }
}
