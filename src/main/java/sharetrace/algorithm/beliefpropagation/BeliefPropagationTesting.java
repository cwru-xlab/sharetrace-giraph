package sharetrace.algorithm.beliefpropagation;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Month;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import sharetrace.algorithm.beliefpropagation.format.FormatUtils;
import sharetrace.algorithm.beliefpropagation.format.vertex.FactorVertex;
import sharetrace.algorithm.beliefpropagation.format.vertex.VariableVertex;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.Occurrence;
import sharetrace.model.identity.UserGroup;
import sharetrace.model.identity.UserId;
import sharetrace.model.score.RiskScore;
import sharetrace.model.score.SendableRiskScores;

public class BeliefPropagationTesting {

  static final Instant MAY_30 = Instant.from(LocalDate.of(2020, Month.MAY, 30));

  static final Instant MAY_31 = Instant.from(LocalDate.of(2020, Month.MAY, 31));

  static final Instant JUNE_1 = Instant.from(LocalDate.of(2020, Month.JUNE, 1));

  static final Instant JUNE_2 = Instant.from(LocalDate.of(2020, Month.JUNE, 2));

  static final Duration CONTACT_12_DURATION = Duration.ofMinutes(20L);

  static final Duration CONTACT_14_DURATION = Duration.ofMinutes(30L);

  static final Duration CONTACT_24_DURATION = Duration.ofMinutes(80L);

  static final Duration CONTACT_34_DURATION = Duration.ofMinutes(60L);

  static final Occurrence OCCURRENCE_12 = Occurrence.of(MAY_30, CONTACT_12_DURATION);

  static final Occurrence OCCURRENCE_14 = Occurrence.of(MAY_31, CONTACT_14_DURATION);

  static final Occurrence OCCURRENCE_24 = Occurrence.of(JUNE_1, CONTACT_24_DURATION);

  static final Occurrence OCCURRENCE_34 = Occurrence.of(JUNE_2, CONTACT_34_DURATION);

  static final UserId USER_1 = UserId.of("1");

  static final UserId USER_2 = UserId.of("2");

  static final UserId USER_3 = UserId.of("3");

  static final UserId USER_4 = UserId.of("4");

  static final UserGroup USERS_1 = UserGroup.builder().addUsers(USER_1).build();

  static final UserGroup USERS_2 = UserGroup.builder().addUsers(USER_2).build();

  static final UserGroup USERS_3 = UserGroup.builder().addUsers(USER_3).build();

  static final UserGroup USERS_4 = UserGroup.builder().addUsers(USER_4).build();

  static final UserGroup USERS_12 = UserGroup.builder().addUsers(USER_1, USER_2).build();

  static final UserGroup USERS_14 = UserGroup.builder().addUsers(USER_1, USER_4).build();

  static final UserGroup USERS_24 = UserGroup.builder().addUsers(USER_2, USER_4).build();

  static final UserGroup USERS_34 = UserGroup.builder().addUsers(USER_3, USER_4).build();

  static final Map<UserGroup, Contact> CONTACT_VERTICES = ImmutableMap.of(
      USERS_12, Contact.of(USER_1, USER_2, Collections.singleton(OCCURRENCE_12)),
      USERS_14, Contact.of(USER_1, USER_4, Collections.singleton(OCCURRENCE_14)),
      USERS_24, Contact.of(USER_2, USER_4, Collections.singleton(OCCURRENCE_24)),
      USERS_34, Contact.of(USER_3, USER_4, Collections.singleton(OCCURRENCE_34)));

  static final Map<UserGroup, Set<RiskScore>> RISK_SCORES = ImmutableMap.of(
      USERS_1, ImmutableSet.of(RiskScore.of(USER_1.getId(), MAY_30, 0.0),
          RiskScore.of(USER_1.getId(), MAY_31, 0.0),
          RiskScore.of(USER_1.getId(), JUNE_1, 0.0),
          RiskScore.of(USER_1.getId(), JUNE_2, 0.2)),
      USERS_2, ImmutableSet.of(RiskScore.of(USER_2.getId(), MAY_30, 1.0),
          RiskScore.of(USER_2.getId(), MAY_31, 0.7),
          RiskScore.of(USER_2.getId(), JUNE_1, 0.7),
          RiskScore.of(USER_2.getId(), JUNE_2, 0.8)),
      USERS_3, ImmutableSet.of(RiskScore.of(USER_3.getId(), MAY_30, 0.2),
          RiskScore.of(USER_3.getId(), MAY_31, 0.4),
          RiskScore.of(USER_3.getId(), JUNE_1, 0.6),
          RiskScore.of(USER_3.getId(), JUNE_2, 0.5)),
      USERS_4, ImmutableSet.of(RiskScore.of(USER_4.getId(), MAY_30, 0.5),
          RiskScore.of(USER_4.getId(), MAY_31, 0.2),
          RiskScore.of(USER_4.getId(), JUNE_1, 0.4),
          RiskScore.of(USER_4.getId(), JUNE_2, 0.6)));

  public static void main(String[] args) throws IOException {
    ObjectMapper objectMapper = FormatUtils.getObjectMapper();
    File variableFile = new File("variable.txt");
    File factorFile = new File("factor.txt");
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(factorFile))) {
      for (Map.Entry<UserGroup, Contact> factor : CONTACT_VERTICES.entrySet()) {
        FactorVertex vertex = FactorVertex.of(factor.getKey(), factor.getValue());
        writer.append(objectMapper.writeValueAsString(vertex));
        writer.newLine();
      }
    }
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(variableFile))) {
      for (Map.Entry<UserGroup, SendableRiskScores> variable : generateUserVertices().entrySet()) {
        VariableVertex vertex = VariableVertex.of(variable.getKey(), variable.getValue());
        writer.write(objectMapper.writeValueAsString(vertex));
        writer.newLine();
      }
    }
    Set<FactorVertex> factors = new HashSet<>();
    try (Stream<String> stream = Files.lines(Paths.get(factorFile.getPath()))) {
      stream.forEach(line -> factors.add(readFactor(objectMapper, line)));
    }
    Set<VariableVertex> variables = new HashSet<>();
    try (Stream<String> stream = Files.lines(Paths.get(variableFile.getPath()))) {
      stream.forEach(line -> variables.add(readVariable(objectMapper, line)));
    }
  }

  static Map<UserGroup, SendableRiskScores> generateUserVertices() {
    Map<UserGroup, SendableRiskScores> riskScores = new HashMap<>();
    for (Map.Entry<UserGroup, Set<RiskScore>> entry : RISK_SCORES.entrySet()) {
      UserGroup user = entry.getKey();
      riskScores.put(user, SendableRiskScores.of(user.getUsers(), entry.getValue()));
    }
    return riskScores;
  }

  private static FactorVertex readFactor(ObjectMapper mapper, String factor) {
    try {
      return mapper.readValue(factor, FactorVertex.class);
    } catch (JsonMappingException e) {
      throw new UncheckedIOException(e);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static VariableVertex readVariable(ObjectMapper mapper, String factor) {
    try {
      return mapper.readValue(factor, VariableVertex.class);
    } catch (JsonMappingException e) {
      throw new UncheckedIOException(e);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
  }
}
