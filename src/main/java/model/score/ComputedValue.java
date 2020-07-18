package model.score;

import java.io.IOException;

/**
 * A generic value that is the result of a computation.
 *
 * @param <N> Numerical type of the value.
 */
public interface ComputedValue<N extends Number> extends Comparable<ComputedValue<N>>
{
    N getValue();

    ComputedValue<N> minus(ComputedValue<N> value) throws IOException;

    ComputedValue<N> abs() throws IOException;
}
