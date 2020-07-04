package main.java.model;

/**
 * A generic value that is the result of a computation.
 *
 * @param <N> Numerical type of the value.
 */
@FunctionalInterface public interface ComputedValue<N extends Number>
{
    N getValue();
}
