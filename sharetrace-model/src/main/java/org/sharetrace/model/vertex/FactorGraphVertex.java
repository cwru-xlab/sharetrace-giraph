package org.sharetrace.model.vertex;

public interface FactorGraphVertex<I, V> extends Vertex<I, V> {

  VertexType getType();
}
