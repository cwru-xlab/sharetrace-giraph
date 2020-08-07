package sharetrace.algorithm.format.vertex;

import sharetrace.model.common.Wrappable;

public interface Vertex<I extends Wrappable<?>, V extends Wrappable<?>> {

  I getVertexId();

  V getVertexValue();
}
