package org.sharetrace.model.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

public final class ShareTraceUtil {

  private static final ObjectMapper MAPPER = new ObjectMapper()
      .registerModules(
          new JavaTimeModule(),
          new Jdk8Module(),
          new ParameterNamesModule(),
          new GuavaModule());

  public static ObjectMapper getMapper() {
    return MAPPER;
  }
}
