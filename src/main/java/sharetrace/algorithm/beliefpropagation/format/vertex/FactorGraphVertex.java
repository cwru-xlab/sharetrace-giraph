package sharetrace.algorithm.beliefpropagation.format.vertex;

import sharetrace.model.common.Wrappable;

public interface FactorGraphVertex<I extends Wrappable<?>, V extends Wrappable<?>> extends Vertex<I, V> {

  VertexType getType();
}
