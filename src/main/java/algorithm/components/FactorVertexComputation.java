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
 *     <li>{@link Vertex} ID: {@link ContactIdWritableComparable}</li>
 *     <li>{@link Vertex} data: {@link ContactDataWritable}</li>
 *     <li>{@link Edge} data: {@link NullWritable}</li>
 *     <li>Input message: {@link ArrayWritable<TemporalRiskScore>}</li>
 *     <li>Output message: {@link TemporalRiskScore}</li>
 * </ul>
 * Each factor vertex receives one or more {@link TemporalRiskScore}s from each of its variable vertices. After
 * computation, the factor vertex sends a single {@link TemporalRiskScore} to each of its variable vertices.
 */
public class FactorVertexComputation
        extends AbstractComputation<ContactIdWritableComparable, ContactDataWritable, NullWritable,
        ArrayWritable<TemporalRiskScoreWritable>, TemporalRiskScoreWritable>
{
    @Override
    public void compute(Vertex<ContactIdWritableComparable, ContactDataWritable, NullWritable> vertex,
                        Iterable<ArrayWritable<TemporalRiskScoreWritable>> iterable) throws IOException
    {
    }
}
