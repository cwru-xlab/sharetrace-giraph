package algorithm.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Value
public class FormatUtils {

  private static final SimpleModule JAVA_TIME_MODULE = new JavaTimeModule();

  private static final Jdk8Module JDK8_MODULE = new Jdk8Module();

  private static final SimpleModule PARAMETER_NAMES_MODULE = new ParameterNamesModule();

  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().registerModules(JAVA_TIME_MODULE, JDK8_MODULE, PARAMETER_NAMES_MODULE);

  public static ObjectMapper getObjectMapper() {
    return OBJECT_MAPPER;
  }
}