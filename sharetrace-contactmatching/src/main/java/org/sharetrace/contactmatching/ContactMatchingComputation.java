package org.sharetrace.contactmatching;

import com.google.common.annotations.VisibleForTesting;
import java.time.Duration;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.location.TemporalLocation;

/**
 * Given two {@link LocationHistory} instances, this algorithm finds a {@link Contact}, if one
 * exists. Two users must be in the same location for a sufficiently long period of time for them to
 * be considered in contact.
 * <p>
 * The algorithm takes as input a list of {@link LocationHistory} instances, each belonging to a
 * distinct user. Since a {@link Contact} is symmetric, only the unique pairs are considered to
 * prevent redundant computation.
 * <p>
 * This algorithm iterates through both histories to find all occurrences. For any given entry, if
 * both {@link LocationHistory} instances have the same location and the occurrence has not begun,
 * the start of the occurrence is the later of the two instances. It is assumed that the user is in
 * the same location until the next location is recorded. Once the occurrence has begun, both {@link
 * LocationHistory} instances are iterated until the locations differ; this marks the end of the
 * occurrence. The earlier of the two {@link LocationHistory} instances is used to indicate the end
 * of the occurrence. If this interval is long enough, the interval is recorded as an official
 * occurrence. This is repeated for all uniques pairs supplied as input to the algorithm. In the
 * case that the end of either {@link LocationHistory} is reached and the occurrence has begun, the
 * same check is made to verify if the occurrence is long enough.
 * <p>
 * Given a list of {@link LocationHistory} instances of size N, the generation of unique pairs takes
 * O(N(N - 1) / 2) = O(N^2). Given two {@link LocationHistory} instances of size H1 and H2, the time
 * to find all occurrences is O(max(H1, H2)). The overall running time is O(max(H1, H2) + N^2).
 */
public class ContactMatchingComputation {

  private static final Duration durationThreshold = Duration.ofMinutes(15L);

  public Set<Contact> compute(List<LocationHistory> histories) {
    return getUniqueEntries(histories.size())
        .stream()
        .map(e -> findContact(histories.get(e.getKey()), histories.get(e.getValue())))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  /*
  Gets all strictly upper triangular entries in a matrix (i.e. unique pairs). For finding all
  unique pairs in a list, put the same list along both axes of a square matrix. To only consider
  unique pairs (i.e. only consider (x, y) and not also (y, x)), only iterate through the entries
  in a strictly upper triangular matrix.
  Ex: matrixSize = 4 -> entries = {(0, 1), (0, 2), (0, 3), (1, 2), (1, 3), (2, 3)}
   */
  public Set<Map.Entry<Integer, Integer>> getUniqueEntries(int matrixSize) {
    return IntStream.range(0, matrixSize)
        .mapToObj(iRow -> getEntriesInRow(matrixSize, iRow))
        .flatMap(Collection::stream)
        .collect(Collectors.toCollection(() -> new HashSet<>(matrixSize * (matrixSize - 1) / 2)));
  }

  // Assumes zero-based numbering and returns strictly upper triangular entries in a row
  private Set<Map.Entry<Integer, Integer>> getEntriesInRow(int matrixSize, int rowIndex) {
    // Ex: matrixSize = 4, rowIndex = 0
    // upperIndex = 3
    int upperIndex = matrixSize - 1;
    // nIndicesInRow = 4 - 0 - 1 = 3
    int nIndicesInRow = matrixSize - rowIndex - 1;
    // rowEntries = {(0, 3), (0, 2), (0, 1)}
    return IntStream.range(0, nIndicesInRow)
        .mapToObj(iIndex -> new AbstractMap.SimpleImmutableEntry<>(rowIndex, upperIndex - iIndex))
        .collect(Collectors.toCollection(() -> new HashSet<>(nIndicesInRow)));
  }

  private Contact findContact(LocationHistory history, LocationHistory otherHistory) {
    Set<Occurrence> occurrences = findOccurrences(history, otherHistory);
    if (occurrences.isEmpty()) {
      return null;
    }
    return Contact.builder()
        .firstUser(history.getId())
        .secondUser(otherHistory.getId())
        .occurrences(occurrences).build();
  }

  private Set<Occurrence> findOccurrences(LocationHistory history, LocationHistory otherHistory) {
    Set<Occurrence> occurrences = new HashSet<>();
    if (eitherHasNoHistory(history, otherHistory) || haveSameId(history, otherHistory)) {
      return occurrences;
    }
    Iterator<TemporalLocation> iter = history.getHistory().iterator();
    Iterator<TemporalLocation> otherIter = otherHistory.getHistory().iterator();
    TemporalLocation location = iter.next();
    TemporalLocation otherLocation = otherIter.next();
    boolean started = false;
    TemporalLocation start = getLater(location, otherLocation);
    while (iter.hasNext() && otherIter.hasNext()) {
      if (location.hasSameLocation(otherLocation)) {
        // Case 1: locations are the same and the occurrence has started
        if (started) {
          location = iter.next();
          otherLocation = otherIter.next();
          // Case 2: locations are the same and the occurrence has not started
        } else {
          started = true;
          start = getLater(location, otherLocation);
        }
      } else {
        // Case 3: locations are different and the occurrence has started
        if (started) {
          started = false;
          TemporalLocation end = getEarlier(location, otherLocation);
          if (isLongEnough(start, end)) {
            occurrences.add(createOccurrence(start, end));
          }
          // Case 4: locations are different and the occurrence has not started
        } else {
          if (location.isBefore(otherLocation)) {
            location = iter.next();
          } else if (otherLocation.isBefore(location)) {
            otherLocation = otherIter.next();
          } else {
            if (ThreadLocalRandom.current().nextInt(2) == 0) {
              location = iter.next();
            } else {
              otherLocation = otherIter.next();
            }
          }
        }
      }
    }
    // Occurrence started but reached the end of either history
    if (started) {
      TemporalLocation end = getEarlier(location, otherLocation);
      if (isLongEnough(start, end)) {
        occurrences.add(createOccurrence(start, end));
      }
    }
    return occurrences;
  }

  private boolean eitherHasNoHistory(LocationHistory history, LocationHistory otherHistory) {
    return history.getHistory().isEmpty() || otherHistory.getHistory().isEmpty();
  }

  private boolean haveSameId(LocationHistory history, LocationHistory otherHistory) {
    return history.getId().equals(otherHistory.getId());
  }

  private TemporalLocation getLater(TemporalLocation location, TemporalLocation otherLocation) {
    return location.isAfter(otherLocation) ? location : otherLocation;
  }

  private TemporalLocation getEarlier(TemporalLocation location, TemporalLocation otherLocation) {
    return location.isBefore(otherLocation) ? location : otherLocation;
  }

  private boolean isLongEnough(TemporalLocation start, TemporalLocation end) {
    return -1 < Duration.between(start.getTime(), end.getTime()).compareTo(getDurationThreshold());
  }

  private Occurrence createOccurrence(TemporalLocation start, TemporalLocation end) {
    Duration duration = Duration.between(start.getTime(), end.getTime());
    return Occurrence.builder().time(start.getTime()).duration(duration).build();
  }

  @VisibleForTesting
  Duration getDurationThreshold() {
    return durationThreshold;
  }
}
