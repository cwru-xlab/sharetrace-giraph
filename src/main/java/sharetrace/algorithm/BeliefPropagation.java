//package algorithm;
//
//import algorithm.format.vertex.FactorVertex;
//import algorithm.format.vertex.VariableVertex;
//import com.fasterxml.jackson.core.JsonProcessingException;
//import com.fasterxml.jackson.databind.JsonMappingException;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import java.io.BufferedWriter;
//import java.io.File;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.UncheckedIOException;
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Paths;
//import java.time.Duration;
//import java.time.Instant;
//import java.time.LocalDate;
//import java.time.LocalTime;
//import java.time.Month;
//import java.time.ZoneOffset;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.SortedSet;
//import java.util.TreeSet;
//import java.util.stream.Stream;
//import model.contact.Contact;
//import model.contact.TemporalOccurrence;
//import model.identity.UserGroup;
//import model.identity.UserId;
//import model.score.SendableRiskScores;
//import model.score.TemporalUserRiskScore;
//
//public class BeliefPropagation {
//
//  static final Instant MAY_30 =
//      Instant.ofEpochSecond(
//          LocalDate.of(2020, Month.MAY, 30).toEpochSecond(LocalTime.of(0, 0), ZoneOffset.UTC));
//
//  static final Instant MAY_31 =
//      Instant.ofEpochMilli(
//          LocalDate.of(2020, Month.MAY, 31).toEpochSecond(LocalTime.of(0, 0), ZoneOffset.UTC));
//
//  static final Instant JUNE_1 =
//      Instant.ofEpochMilli(
//          LocalDate.of(2020, Month.JUNE, 1).toEpochSecond(LocalTime.of(0, 0), ZoneOffset.UTC));
//
//  static final Instant JUNE_2 =
//      Instant.ofEpochMilli(
//          LocalDate.of(2020, Month.JUNE, 2).toEpochSecond(LocalTime.of(0, 0), ZoneOffset.UTC));
//
//  static final Duration CONTACT_12_DURATION = Duration.ofMinutes(20L);
//
//  static final Duration CONTACT_14_DURATION = Duration.ofMinutes(30L);
//
//  static final Duration CONTACT_24_DURATION = Duration.ofMinutes(80L);
//
//  static final Duration CONTACT_34_DURATION = Duration.ofMinutes(60L);
//
//  static final TemporalOccurrence OCCURRENCE_12 = TemporalOccurrence
//      .of(MAY_30, CONTACT_12_DURATION);
//
//  static final TemporalOccurrence OCCURRENCE_14 = TemporalOccurrence
//      .of(MAY_31, CONTACT_14_DURATION);
//
//  static final TemporalOccurrence OCCURRENCE_24 = TemporalOccurrence
//      .of(JUNE_1, CONTACT_24_DURATION);
//
//  static final TemporalOccurrence OCCURRENCE_34 = TemporalOccurrence
//      .of(JUNE_2, CONTACT_34_DURATION);
//
//  static final UserId USER_1 = UserId.of("1");
//
//  static final UserId USER_2 = UserId.of("2");
//
//  static final UserId USER_3 = UserId.of("3");
//
//  static final UserId USER_4 = UserId.of("4");
//
//  static final UserGroup USERS_1 = UserGroup.of(USER_1);
//
//  static final UserGroup USERS_2 = UserGroup.of(USER_2);
//
//  static final UserGroup USERS_3 = UserGroup.of(USER_3);
//
//  static final UserGroup USERS_4 = UserGroup.of(USER_4);
//
//  static final UserGroup USERS_12 = UserGroup.of(USER_1, USER_2);
//
//  static final UserGroup USERS_14 = UserGroup.of(USER_1, USER_4);
//
//  static final UserGroup USERS_24 = UserGroup.of(USER_2, USER_4);
//
//  static final UserGroup USERS_34 = UserGroup.of(USER_3, USER_4);
//
//  static final Map<UserGroup, Contact> CONTACT_VERTICES = Map.of(USERS_12,
//      Contact.of(USER_1, USER_2, OCCURRENCE_12),
//      USERS_14,
//      Contact.of(USER_1, USER_4, OCCURRENCE_14),
//      USERS_24,
//      Contact.of(USER_2, USER_4, OCCURRENCE_24),
//      USERS_34,
//      Contact.of(USER_3, USER_4, OCCURRENCE_34));
//
//  static final Map<UserGroup, List<TemporalRiskScore>> RISK_SCORES = Map.of(USERS_1,
//      List.of(TemporalRiskScore.of(MAY_30, 0.0),
//          TemporalRiskScore.of(MAY_31, 0.0),
//          TemporalRiskScore.of(JUNE_1, 0.0),
//          TemporalRiskScore.of(JUNE_2,
//              0.2)),
//      USERS_2,
//      List.of(TemporalRiskScore.of(MAY_30, 1.0),
//          TemporalRiskScore.of(MAY_31, 0.7),
//          TemporalRiskScore.of(JUNE_1, 0.7),
//          TemporalRiskScore.of(JUNE_2,
//              0.8)),
//      USERS_3,
//      List.of(TemporalRiskScore.of(MAY_30, 0.2),
//          TemporalRiskScore.of(MAY_31, 0.4),
//          TemporalRiskScore.of(JUNE_1, 0.6),
//          TemporalRiskScore.of(JUNE_2,
//              0.5)),
//      USERS_4,
//      List.of(TemporalRiskScore.of(MAY_30, 0.5),
//          TemporalRiskScore.of(MAY_31, 0.2),
//          TemporalRiskScore.of(JUNE_1, 0.4),
//          TemporalRiskScore.of(JUNE_2,
//              0.6)));
//
//  public static void main(String[] args) throws IOException {
//    ObjectMapper objectMapper = new ObjectMapper();
//    File variableFile = new File("out/variable.txt");
//    File factorFile = new File("out/factor.txt");
//    Charset utf8 = StandardCharsets.UTF_8;
//    try (BufferedWriter writer = new BufferedWriter(new FileWriter(factorFile, utf8))) {
//      for (Map.Entry<UserGroup, Contact> factor : CONTACT_VERTICES.entrySet()) {
//        FactorVertex vertex = FactorVertex.of(factor.getKey(), factor.getValue());
//        writer.write(objectMapper.writeValueAsString(vertex));
//        writer.newLine();
//      }
//    }
//    try (BufferedWriter writer = new BufferedWriter(new FileWriter(variableFile, utf8))) {
//      for (Map.Entry<UserGroup, SendableRiskScores> variable : generateUserVertices().entrySet()) {
//        VariableVertex vertex = VariableVertex.of(variable.getKey(), variable.getValue());
//        writer.write(objectMapper.writeValueAsString(vertex));
//        writer.newLine();
//      }
//    }
//    Set<FactorVertex> factors = new HashSet<>();
//    try (Stream<String> stream = Files.lines(Paths.get(factorFile.getPath()))) {
//      stream.forEach(line -> factors.add(readFactor(objectMapper, line)));
//    }
//    Set<VariableVertex> variables = new HashSet<>();
//    try (Stream<String> stream = Files.lines(Paths.get(variableFile.getPath()))) {
//      stream.forEach(line -> variables.add(readVariable(objectMapper, line)));
//    }
//  }
//
//  static Map<UserGroup, SendableRiskScores> generateUserVertices() {
//    Map<UserGroup, SendableRiskScores> riskScores = new HashMap<>();
//    for (Map.Entry<UserGroup, List<TemporalRiskScore>> entry : RISK_SCORES.entrySet()) {
//      UserGroup user = entry.getKey();
//      UserId userId = user.first();
//      SortedSet<TemporalUserRiskScore> sortedRiskScores = generateSortedRiskScores(userId,
//          entry.getValue());
//      riskScores.put(user, SendableRiskScores.of(user, sortedRiskScores));
//    }
//    return riskScores;
//  }
//
//  private static SortedSet<TemporalUserRiskScore> generateSortedRiskScores(UserId userId,
//      Iterable<TemporalRiskScore> riskScores) {
//    SortedSet<TemporalUserRiskScore> sortedRiskScores = new TreeSet<>();
//    for (TemporalRiskScore riskScore : riskScores) {
//      sortedRiskScores.add(
//          TemporalUserRiskScore.of(userId, riskScore.getTimeUpdated(), riskScore.getRiskScore()));
//    }
//    return sortedRiskScores;
//  }
//
//  private static FactorVertex readFactor(ObjectMapper mapper, String factor) {
//    try {
//      return mapper.readValue(factor, FactorVertex.class);
//    } catch (JsonMappingException e) {
//      throw new UncheckedIOException(e);
//    } catch (JsonProcessingException e) {
//      throw new UncheckedIOException(e);
//    }
//  }
//
//  private static VariableVertex readVariable(ObjectMapper mapper, String factor) {
//    try {
//      return mapper.readValue(factor, VariableVertex.class);
//    } catch (JsonMappingException e) {
//      throw new UncheckedIOException(e);
//    } catch (JsonProcessingException e) {
//      throw new UncheckedIOException(e);
//    }
//  }
//}
