package model.identity;

/**
 * A property of anything used as an identifier.
 *
 * @param <T> Type of identifier.
 */
public interface Identifiable<T> extends Comparable<Identifiable<T>> {

  T getId();
}
