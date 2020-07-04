package main.java.model;

/**
 * A generic value that is the result of a computation.
 *
 * @param <N> Numerical type of the value.
 */
public interface ComputedValue<N extends Number> extends Comparable<N>
{
    N getValue();
}
