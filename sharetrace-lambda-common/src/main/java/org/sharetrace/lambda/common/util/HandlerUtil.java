package org.sharetrace.lambda.common.util;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.common.base.Preconditions;

public final class HandlerUtil {

  private static final String INVALID_INPUT_MSG = "Input must not be null";

  private static final String INVALID_CONTEXT_MSG = "Context must not be null";

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final String SUCCESS_200_OK = "200 OK";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  public static void logEnvironment(Object input, Context context) {
    Preconditions.checkNotNull(input, INVALID_INPUT_MSG);
    Preconditions.checkNotNull(context, INVALID_CONTEXT_MSG);
    LambdaLogger logger = context.getLogger();
    String environmentVariablesLog = STRING_BUILDER
        .append(ENVIRONMENT_VARIABLES)
        .append(System.getenv())
        .toString();
    logger.log(environmentVariablesLog);
    resetStringBuilder();

    String contextLog = STRING_BUILDER
        .append(CONTEXT)
        .append(context)
        .toString();
    logger.log(contextLog);
    resetStringBuilder();

    String eventLog = STRING_BUILDER
        .append(EVENT)
        .append(input)
        .toString();
    logger.log(eventLog);
    resetStringBuilder();

    String eventTypeLog = STRING_BUILDER
        .append(EVENT_TYPE)
        .append(input.getClass().getSimpleName())
        .toString();
    logger.log(eventTypeLog);
    resetStringBuilder();
  }

  private static void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }

  public static String getEnvironmentVariable(String envVarKey) {
    return System.getenv(envVarKey);
  }

  public static String get200Ok() {
    return SUCCESS_200_OK;
  }
}
