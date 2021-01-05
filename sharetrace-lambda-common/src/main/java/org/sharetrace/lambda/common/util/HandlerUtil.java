package org.sharetrace.lambda.common.util;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class HandlerUtil {

  // Logging messages
  private static final String CANNOT_WRITE_TO_S3_MSG = "Unable to write to S3: \n";
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";
  private static final String INCOMPLETE_REQUEST_MSG = "Failed to complete short-lived token request: \n";
  private static final String FAILED_TO_SERIALIZE_MSG = "Failed to serialize request body: \n";
  private static final String NO_WORKERS_MSG = "No worker functions were found. Exiting handler";
  private static final String CANNOT_DESERIALIZE_MSG = "Unable to deserialize: \n";
  private static final String CANNOT_READ_FROM_PDA_MSG = "Unable to read data from PDA: \n";
  private static final String CANNOT_WRITE_TO_PDA_MSG = "Unable to write data to PDA: \n";
  private static final String HAT_DOES_NOT_EXIST_MSG = "Hat does not exist: \n";

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final String SUCCESS_200_OK = "200 OK";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  public static File createRandomFile(String prefix, String fileFormat) {
    return new File(prefix + UUID.randomUUID().toString() + fileFormat);
  }

  public static void logMessage(LambdaLogger logger, String message) {
    Preconditions.checkNotNull(logger);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(message));
  }

  public static void logException(LambdaLogger logger, Exception exception, String prefixedMsg) {
    Preconditions.checkNotNull(logger);
    Preconditions.checkNotNull(exception);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(prefixedMsg));
    logger.log(prefixedMsg + exception.getMessage());
    logger.log(Arrays.toString(exception.getStackTrace()));
  }

  public static void logException(LambdaLogger logger, Exception exception) {
    Preconditions.checkNotNull(logger);
    Preconditions.checkNotNull(exception);
    logger.log(exception.getMessage());
    logger.log(Arrays.toString(exception.getStackTrace()));
  }

  public static void logEnvironment(Object input, Context context) {
    Preconditions.checkNotNull(input);
    Preconditions.checkNotNull(context);
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

  public static String getEnvVar(String key, LambdaLogger logger) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key));
    Preconditions.checkNotNull(logger);
    String value = null;
    try {
      value = System.getenv(key);
    } catch (NullPointerException e) {
      logException(logger, e, CANNOT_FIND_ENV_VAR_MSG);
      System.exit(1);
    }
    return value;
  }

  public static List<String> getWorkers(Collection<String> workerKeys, LambdaLogger logger) {
    Preconditions.checkNotNull(workerKeys);
    Preconditions.checkArgument(!workerKeys.isEmpty());
    Preconditions.checkNotNull(logger);
    List<String> workers = workerKeys
        .stream()
        .map(k -> getEnvVar(k, logger))
        .collect(Collectors.toList());
    if (workers.isEmpty()) {
      logger.log(NO_WORKERS_MSG);
      System.exit(1);
    }
    return workers;
  }

  private static void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }

  public static String getCannotWriteToS3Msg() {
    return CANNOT_WRITE_TO_S3_MSG;
  }

  public static String getCannotFindEnvVarMsg() {
    return CANNOT_FIND_ENV_VAR_MSG;
  }

  public static String getMalformedUrlMsg() {
    return MALFORMED_URL_MSG;
  }

  public static String getIncompleteRequestMsg() {
    return INCOMPLETE_REQUEST_MSG;
  }

  public static String getFailedToSerializeMsg() {
    return FAILED_TO_SERIALIZE_MSG;
  }

  public static String getNoWorkersMsg() {
    return NO_WORKERS_MSG;
  }

  public static String getCannotDeserializeMsg() {
    return CANNOT_DESERIALIZE_MSG;
  }

  public static String getCannotReadFromPdaMsg() {
    return CANNOT_READ_FROM_PDA_MSG;
  }

  public static String getCannotWriteToPdaMsg() {
    return CANNOT_WRITE_TO_PDA_MSG;
  }

  public static String getHatDoesNotExistMsg() {
    return HAT_DOES_NOT_EXIST_MSG;
  }

  public static String get200Ok() {
    return SUCCESS_200_OK;
  }
}
