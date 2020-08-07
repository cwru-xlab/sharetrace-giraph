package sharetrace.model.common;

import org.apache.hadoop.io.Writable;

/**
 * An interface used allow a type that does not implement {@link Writable} be wrapped in a type that
 * does.
 *
 * @param <T> Type that implements {@link Writable}.
 */
@FunctionalInterface
public interface Wrappable<T extends Writable> {

  T wrap();
}
