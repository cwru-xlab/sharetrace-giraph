package main.java.model;

/**
 * A property of anything used as an identifier.
 *
 * @param <T> Type of identifier.
 */
@FunctionalInterface
public interface Identifiable<T>
{
    T getId();
}
