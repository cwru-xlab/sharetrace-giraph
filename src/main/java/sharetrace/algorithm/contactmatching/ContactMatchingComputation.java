package sharetrace.algorithm.contactmatching;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.Occurrence;
import sharetrace.model.location.LocationHistory;
import sharetrace.model.location.TemporalLocation;

/**
 * Given two {@link LocationHistory} instances, this algorithm finds a {@link Contact}, if one
 * exists. Two users must be in the same location for a sufficiently long period of time for them to
 * be considered in contact.
 * <p>
 * The algorithm takes as input a list of {@link LocationHistory} instances, each belonging to a
 * distinct user. Since a {@link Contact} is symmetric, only the unique pairs are considered to
 * prevent redundant computation.
 * <p>
 * Imagining two {@link LocationHistory} instances on a timeline, the algorithm iterates through
 * both histories once to find all occurrences. For any given entry, if both {@link LocationHistory}
 * instances have the same location and the occurrence has not begun, the start of the occurrence is
 * the later of the two instances. It is assumed that the user is in the same location until the
 * next location is recorded. Once the occurrence has begun, both {@link LocationHistory} instances
 * are iterated until the locations differ; this marks the end of the occurrence. The earlier of the
 * two {@link LocationHistory} instances is used to indicate the end of the occurrence. If this
 * interval is long enough, the interval is recorded as an official occurrence. This is repeated for
 * all uniques pairs supplied as input to the algorithm.
 * <p>
 * Given a list of {@link LocationHistory} instances of size N, the generation of unique pairs takes
 * O(N(N - 1) / 2) = O(N^2). Given two {@link LocationHistory} instances of size H1 and H2, the time
 * to find all occurrences is O(max(H1, H2)). The overall running time is O(max(H1, H2) + N^2).
 */
public class ContactMatchingComputation {

  private static final Duration DURATION_THRESHOLD = Duration.ofMinutes(15L);

  public Collection<Contact> compute(List<LocationHistory> histories) {
    return getStrictUpperTriangularMatrixEntries(histories.size())
        .parallelStream()
        .map(e -> findContact(histories.get(e.getKey()), histories.get(e.getValue())))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }

  // Gets all strictly upper triangular entries in a matrix (i.e. unique pairs)
  private Set<Map.Entry<Integer, Integer>> getStrictUpperTriangularMatrixEntries(int matrixSize) {
    Set<Map.Entry<Integer, Integer>> entries = new HashSet<>(matrixSize * (matrixSize - 1) / 2);
    for (int iRow = 0; iRow < matrixSize; iRow++) {
      entries.addAll(getEntriesInRow(matrixSize, iRow));
    }
    return entries;
  }

  // Assumes zero-based numbering and returns strictly upper triangular entries in a row
  private Set<Map.Entry<Integer, Integer>> getEntriesInRow(int matrixSize, int rowIndex) {
    int oneBasedRowIndex = rowIndex + 1;
    int upperIndex = oneBasedRowIndex * matrixSize;
    int nIndicesInRow = oneBasedRowIndex * (matrixSize - rowIndex - 1);
    Set<Map.Entry<Integer, Integer>> rowEntries = new HashSet<>(nIndicesInRow);
    for (int iIndex = 0; iIndex < nIndicesInRow; iIndex++) {
      rowEntries.add(Map.entry(rowIndex, upperIndex - iIndex));
    }
    return rowEntries;
  }

  private Contact findContact(LocationHistory history, LocationHistory otherHistory) {
    Set<Occurrence> occurrences = findOccurrences(history, otherHistory);
    if (occurrences.isEmpty()) {
      return null;
    }
    return Contact.builder()
        .setFirstUser(history.getId())
        .setSecondUser(otherHistory.getId())
        .setOccurrences(occurrences).build();
  }

  private Set<Occurrence> findOccurrences(LocationHistory history, LocationHistory otherHistory) {
    Iterator<TemporalLocation> iter = history.getHistory().iterator();
    Iterator<TemporalLocation> otherIter = otherHistory.getHistory().iterator();
    TemporalLocation location = iter.next();
    TemporalLocation otherLocation = otherIter.next();
    boolean started = false;
    TemporalLocation start = getLater(location, otherLocation);
    Set<Occurrence> occurrences = new HashSet<>();
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
    return occurrences;
  }

  private TemporalLocation getLater(TemporalLocation location, TemporalLocation otherLocation) {
    return location.isAfter(otherLocation) ? location : otherLocation;
  }

  private TemporalLocation getEarlier(TemporalLocation location, TemporalLocation otherLocation) {
    return location.isBefore(otherLocation) ? location : otherLocation;
  }

  private boolean isLongEnough(TemporalLocation start, TemporalLocation end) {
    return -1 < Duration.between(start.getTime(), end.getTime()).compareTo(DURATION_THRESHOLD);
  }

  private Occurrence createOccurrence(TemporalLocation start, TemporalLocation end) {
    Duration duration = Duration.between(start.getTime(), end.getTime());
    return Occurrence.of(start.getTime(), duration);
  }
}
