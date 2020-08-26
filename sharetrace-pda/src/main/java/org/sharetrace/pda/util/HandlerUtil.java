package org.sharetrace.pda.util;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.sharetrace.model.util.ShareTraceUtil;

public final class HandlerUtil {

  private static final String INVALID_INPUT_MSG = "Input must not be null";

  private static final String INVALID_CONTEXT_MSG = "Context must not be null";

  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to find environment variable: \n";

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  public static void logEnvironment(Object input, Context context) {
    Preconditions.checkNotNull(input, INVALID_INPUT_MSG);
    Preconditions.checkNotNull(context, INVALID_CONTEXT_MSG);
    LambdaLogger logger = context.getLogger();
    try {
      String environmentVariablesLog = STRING_BUILDER
          .append(ENVIRONMENT_VARIABLES)
          .append(MAPPER.writeValueAsString(System.getenv()))
          .toString();
      logger.log(environmentVariablesLog);
      resetStringBuilder();

      String contextLog = STRING_BUILDER
          .append(CONTEXT)
          .append(MAPPER.writeValueAsString(context))
          .toString();
      logger.log(contextLog);
      resetStringBuilder();

      String eventLog = STRING_BUILDER
          .append(EVENT)
          .append(MAPPER.writeValueAsString(input))
          .toString();
      logger.log(eventLog);
      resetStringBuilder();

      String eventTypeLog = STRING_BUILDER
          .append(EVENT_TYPE)
          .append(input.getClass().getSimpleName())
          .toString();
      logger.log(eventTypeLog);
      resetStringBuilder();
    } catch (JsonProcessingException e) {
      logger.log(e.getMessage());
    }
  }

  private static void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }

  public static String getEnvironmentVariable(String envVarKey, LambdaLogger logger) {
    String envVarValue = null;
    try {
      envVarValue = System.getenv(envVarKey);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
    }
    return envVarValue;
  }

}
