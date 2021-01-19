package org.sharetrace.model.score;

/**
 * An interface that defines an object that has a sender and a message.
 *
 * @param <T> Type of the sender.
 * @param <U> Type of the message.
 */
public interface Sendable<T, U> {

  T getSender();

  U getMessage();

}
