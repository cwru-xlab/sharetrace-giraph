package org.sharetrace.lambda.common.util;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.io.File;
import java.util.Arrays;
import java.util.UUID;

public final class HandlerUtil {

  // Logging messages
  private static final String INVALID_INPUT_MSG = "Input must not be null";
  private static final String INVALID_CONTEXT_MSG = "Context must not be null";
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


  private static final String NULL_LOGGER_MSG = "Logger must not be null to log a message or error";
  private static final String NULL_EXCEPTION_MSG = "Exception must not be null to log it";
  private static final String NULL_STRING_MSG = "Logged message must not be null or empty String";

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
    Preconditions.checkNotNull(logger, NULL_LOGGER_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(message), NULL_STRING_MSG);
  }

  public static void logException(LambdaLogger logger, Exception exception, String prefixedMsg) {
    Preconditions.checkNotNull(logger, NULL_LOGGER_MSG);
    Preconditions.checkNotNull(exception, NULL_EXCEPTION_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(prefixedMsg), NULL_STRING_MSG);
    logger.log(prefixedMsg + exception.getMessage());
    logger.log(Arrays.toString(exception.getStackTrace()));
  }

  public static void logException(LambdaLogger logger, Exception exception) {
    Preconditions.checkNotNull(logger, NULL_LOGGER_MSG);
    Preconditions.checkNotNull(exception, NULL_EXCEPTION_MSG);
    logger.log(exception.getMessage());
    logger.log(Arrays.toString(exception.getStackTrace()));
  }

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
