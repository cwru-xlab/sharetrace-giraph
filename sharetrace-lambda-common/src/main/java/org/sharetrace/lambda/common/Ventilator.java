package org.sharetrace.lambda.common;

import java.util.Collection;
import java.util.List;

/**
 * Provides the general behavior of a function that invokes the fan-out to worker functions.
 *
 * @param <T> Type of the payload processed by a worker function.
 */
public interface Ventilator<T> {

  void handleRequest();

  void invokeWorker(String worker, Collection<? extends T> payload);

  List<String> getWorkers();
}
