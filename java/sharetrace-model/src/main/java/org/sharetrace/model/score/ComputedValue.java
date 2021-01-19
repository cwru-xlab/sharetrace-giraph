package org.sharetrace.model.score;

/**
 * Any value that is a result of a computation
 *
 * @param <N> Number type of the computation result.
 */
@FunctionalInterface
public interface ComputedValue<N extends Number> {

  N getValue();
}
