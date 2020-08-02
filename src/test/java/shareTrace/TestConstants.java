package shareTrace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import java.time.Duration;
import java.time.Instant;

public final class TestConstants {

    private static final String USER_ID_1_STRING = "USER_ID_1";

    private static final String USER_ID_2_STRING = "USER_ID_2";

    private static final Instant INSTANT_1 = Instant.ofEpochMilli(1234567890L);

    private static final Instant INSTANT_2 = Instant.ofEpochMilli(1357924680L);

    private static final Duration DURATION_1 = Duration.ofMillis(1000L);

    private static final Duration DURATION_2 = Duration.ofMillis(2000L);

    private static final SimpleModule JAVA_TIME_MODULE = new JavaTimeModule();

    private static final Jdk8Module JDK8_MODULE = new Jdk8Module();

    private static final SimpleModule PARAMETER_NAMES_MODULE = new ParameterNamesModule();

    private static final double MIN_RISK_SCORE = 0.0;

    private static final double MAX_RISK_SCORE = 1.0;
    private static final ObjectMapper OBJECT_MAPPER =
        new ObjectMapper().registerModules(JAVA_TIME_MODULE, JDK8_MODULE, PARAMETER_NAMES_MODULE);

    public static Instant getInstant1() {
        return INSTANT_1;
    }

    public static Instant getInstant2() {
        return INSTANT_2;
    }

    public static Duration getDuration1() {
        return DURATION_1;
    }

    public static Duration getDuration2() {
        return DURATION_2;
    }

    public static String getUserId1String() {
        return USER_ID_1_STRING;
    }

    public static String getUserId2String() {
        return USER_ID_2_STRING;
    }

    public static ObjectMapper getObjectMapper() {
        return OBJECT_MAPPER;
    }

    public static double getMinRiskScore() {
        return MIN_RISK_SCORE;
    }

    public static double getMaxRiskScore() {
        return MAX_RISK_SCORE;
    }
}
