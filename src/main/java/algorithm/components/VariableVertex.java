package main.java.algorithm.components;

import org.apache.giraph.graph.DefaultVertex;
import org.apache.hadoop.io.NullWritable;

public class VariableVertex
        extends DefaultVertex<UserRiskScoreWritableComparable,
        UserRiskScoreWritable, NullWritable>
{}
