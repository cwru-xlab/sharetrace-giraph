package org.sharetrace.beliefpropagation.format;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FormatUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(FormatUtils.class);

  private static final SimpleModule JAVA_TIME_MODULE = new JavaTimeModule();

  private static final Jdk8Module JDK8_MODULE = new Jdk8Module();

  private static final SimpleModule PARAMETER_NAMES_MODULE = new ParameterNamesModule();

  private static final GuavaModule GUAVA_MODULE = new GuavaModule();

  private static final ObjectMapper MAPPER = new ObjectMapper().registerModules(
      JAVA_TIME_MODULE,
      JDK8_MODULE,
      PARAMETER_NAMES_MODULE,
      GUAVA_MODULE);

  private FormatUtils() {
  }

  public static ObjectMapper getObjectMapper() {
    return MAPPER;
  }
}