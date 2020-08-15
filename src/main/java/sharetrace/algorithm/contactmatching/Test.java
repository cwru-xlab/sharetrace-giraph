package sharetrace.algorithm.contactmatching;

import com.google.common.collect.ImmutableList;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import sharetrace.model.identity.UserId;
import sharetrace.model.location.LocationHistory;
import sharetrace.model.location.TemporalLocation;

public class Test {

  private static final List<String> LOCATIONS = ImmutableList.of("l1", "l2", "l3", "l4", "l5");

  public static void main(String[] args) throws IOException {
    ContactMatchingComputation algorithm = new ContactMatchingComputation();
    int nHistories = 2000;
    int nLcocations = 50;
    List<LocationHistory> histories = generateLocationHistories(nHistories, nLcocations);
    try (BufferedWriter writer = new BufferedWriter(
        new FileWriter(new File("contact_matching.txt")))) {
      long start = System.nanoTime();
      algorithm.compute(histories);
      long stop = System.nanoTime();
      writer.write(new StringBuilder().append("nHistories: ")
          .append(nHistories)
          .append("; nLocations: ")
          .append(nLcocations)
          .append(" runtime (ms): ")
          .append((stop - start) / 10E6)
          .toString());

//    ContactMatchingComputation algorithm = new ContactMatchingComputation();
//    try (BufferedWriter writer =
//        new BufferedWriter(new FileWriter(new File("run_durations.txt"), true))) {
//      writer.write("nHistories,nLocations,time (ns) \n");
//      for (int nHistories = 1; nHistories < 1000; nHistories++) {
//        for (int nLocations = 1; nLocations < 100; nLocations++) {
//          List<LocationHistory> histories = generateLocationHistories(nHistories, nLocations);
//          long start = System.nanoTime();
//          algorithm.compute(histories);
//          long stop = System.nanoTime();
//          writer.write(nHistories + "," + nLocations + "," + (stop - start) + "\n");
//        }
//      }
//    }
    }
  }
    public static List<LocationHistory> generateLocationHistories ( int nHistories, int nLocations){
      List<LocationHistory> histories = new ArrayList<>(nHistories);
      for (int iHistory = 0; iHistory < nHistories; iHistory++) {
        UserId userId = UserId.of("u" + iHistory);
        histories.add(LocationHistory.of(userId, generateTemporalLocation(nLocations)));
      }
      return histories;
    }

    public static Set<TemporalLocation> generateTemporalLocation ( int nLocations){
      Set<TemporalLocation> locations = new HashSet<>();
      Instant instant = Instant.now();
      for (int iLocation = 0; iLocation < nLocations; iLocation++) {
        instant = instant.plus(Duration.ofMinutes(new Random().nextInt(30)));
        int locationInd = new Random().nextInt(5);
        locations.add(TemporalLocation.of(LOCATIONS.get(locationInd), instant));
      }
      return locations;
    }
  }
