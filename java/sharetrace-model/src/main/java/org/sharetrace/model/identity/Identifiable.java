package org.sharetrace.model.identity;

/**
 * An interface that indicates a type that has an identifier.
 *
 * @param <T> Type of the identifier.
 */
@FunctionalInterface
public interface Identifiable<T> {

  T getId();
}
