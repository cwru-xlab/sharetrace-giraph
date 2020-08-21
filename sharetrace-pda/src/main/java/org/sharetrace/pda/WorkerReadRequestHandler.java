package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;
import java.util.stream.Collectors;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.location.RawLocationHistory;
import org.sharetrace.model.location.RawTemporalLocation;
import org.sharetrace.model.location.TemporalLocation;
import org.sharetrace.model.pda.HatContext;
import org.sharetrace.model.pda.request.AbstractPdaReadRequestParameters.Ordering;
import org.sharetrace.model.pda.request.AbstractPdaRequestUrl.Operation;
import org.sharetrace.model.pda.request.ContractedPdaReadRequest;
import org.sharetrace.model.pda.request.ContractedPdaReadRequestBody;
import org.sharetrace.model.pda.request.PdaReadRequestParameters;
import org.sharetrace.model.pda.request.PdaRequestUrl;
import org.sharetrace.model.pda.response.PdaReadResponse;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.util.ShareTraceUtil;

public class WorkerReadRequestHandler implements
    RequestHandler<ContractedPdaReadRequestBody, String> {

  private static final String INVALID_INPUT_MSG = "Input must not be null";

  private static final String INVALID_CONTEXT_MSG = "Context must not be null";

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final String LOCATIONS_ENDPOINT = "locationsEndpoint";

  private static final String LOCATIONS_NAMESPACE = "locationsNamespace";

  private static final String CANNOT_FIND_ENDPOINT_MSG =
      "Unable to find endpoint. Check environment variables: \n";

  private static final String CANNOT_FIND_NAMESPACE_MSG =
      "Unable to find namespace. Check environment variables: \n";

  private static final String IS_SANDBOX = "isSandbox";

  private static final String MISSING_IS_SANDBOX_MSG =
      "Unable to find isSandbox. Check environment variables: \n";

  private static final String ORDER_BY = "TIMESTAMP";

  private static final String HAT_CONTEXT_BUCKET = "s3://sharetrace/hatContext";

  private static final String CANNOT_DESERIALIZE = "Unable to deserialize: \n";

  private static final String LOCATIONS_BUCKET = "s3://sharetrace/locations";

  private static final String UNABLE_TO_WRITE_TO_S3_MSG = "Unable to write to S3: \n";

  private static final String CANNOT_READ_FROM_PDA_MSG = "Unable to read data from PDA: \n";

  private static final String SCORE_ENDPOINT = "scoreEndpoint";

  private static final String SCORE_NAMESPACE = "scoreNamespace";

  private static final String SCORE_BUCKET = "s3://sharetrace/score/input";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  @Override
  public String handleRequest(ContractedPdaReadRequestBody input, Context context) {
    Preconditions.checkNotNull(input, INVALID_INPUT_MSG);
    Preconditions.checkNotNull(context, INVALID_CONTEXT_MSG);
    log(input, context);
    LambdaLogger logger = context.getLogger();

    PdaRequestUrl.Builder commonBuilder = getCommonUrlBuilder(logger);
    String hatName = input.getHatName();

    String locsEndpoint = getLocationsEndpoint(logger);
    String locsNamespace = getLocationNamespace(logger);
    PdaRequestUrl locsUrl = getPdaRequestUrl(commonBuilder, locsEndpoint, locsNamespace);
    PdaReadRequestParameters parameters = PdaReadRequestParameters.builder()
        .orderBy(ORDER_BY)
        .ordering(Ordering.ASCENDING)
        .skipAmount(getSkipAmount(hatName, logger))
        .build();
    ContractedPdaReadRequestBody body = ContractedPdaReadRequestBody.builder()
        .contractId(input.getContractId())
        .hatName(input.getHatName())
        .shortLivedToken(input.getShortLivedToken())
        .parameters(parameters)
        .build();
    ContractedPdaReadRequest locationsRequest = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(locsUrl)
        .readRequestBody(body)
        .build();
    PdaReadResponse locationsResponse = getPdaReadResponse(locationsRequest, logger);
    writeLocationsToS3(hatName, locationsResponse, logger);

    String scoreEndpoint = getScoreEndpoint(logger);
    String scoreNamespace = getScoreNamespace(logger);
    PdaRequestUrl scoreUrl = getPdaRequestUrl(commonBuilder, scoreEndpoint, scoreNamespace);
    ContractedPdaReadRequest scoreRequest = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(scoreUrl)
        .build();
    PdaReadResponse scoreResponse = getPdaReadResponse(scoreRequest, logger);
    writeScoreToS3(hatName, scoreResponse, logger);

    return null;
  }

  private void log(ContractedPdaReadRequestBody input, Context context) {
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

  private void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }

  private PdaRequestUrl.Builder getCommonUrlBuilder(LambdaLogger logger) {
    String isSandbox = null;
    PdaRequestUrl.Builder commonUrlBuilder = null;
    try {
      isSandbox = System.getenv(IS_SANDBOX);
      boolean sandbox = Boolean.parseBoolean(isSandbox);
      commonUrlBuilder = PdaRequestUrl.builder()
          .contracted(true)
          .operation(Operation.READ)
          .sandbox(sandbox);
    } catch (NullPointerException e) {
      logger.log(MISSING_IS_SANDBOX_MSG + e.getMessage());
      System.exit(1);
    }
    return commonUrlBuilder;
  }

  private String getLocationsEndpoint(LambdaLogger logger) {
    String endpoint = null;
    try {
      endpoint = System.getenv(LOCATIONS_ENDPOINT);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENDPOINT_MSG + e.getMessage());
      System.exit(1);
    }
    return endpoint;
  }

  private String getLocationNamespace(LambdaLogger logger) {
    String namespace = null;
    try {
      namespace = System.getenv(LOCATIONS_NAMESPACE);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_NAMESPACE_MSG + e.getMessage());
      System.exit(1);
    }
    return namespace;
  }

  private PdaReadResponse getPdaReadResponse(ContractedPdaReadRequest readRequest,
      LambdaLogger logger) {
    PdaReadResponse response = null;
    try {
      response = PDA_CLIENT.readFromContractedPda(readRequest);
    } catch (IOException e) {
      logger.log(CANNOT_READ_FROM_PDA_MSG + e.getMessage());
      System.exit(1);
    }
    return response;
  }

  private PdaRequestUrl getPdaRequestUrl(PdaRequestUrl.Builder builder, String endpoint,
      String namespace) {
    return builder
        .endpoint(endpoint)
        .namespace(namespace)
        .build();
  }

  private int getSkipAmount(String hatName, LambdaLogger logger) {
    GetObjectRequest objectRequest = new GetObjectRequest(HAT_CONTEXT_BUCKET, hatName);
    S3Object locationsObject = S3_CLIENT.getObject(objectRequest);
    S3ObjectInputStream inputStream = locationsObject.getObjectContent();
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
    int skipAmount = 0;
    try {
      HatContext hatContext = MAPPER.readValue(reader.readLine(), HatContext.class);
      skipAmount = hatContext.getNumRecordsRead();
    } catch (IOException e) {
      logger.log(CANNOT_DESERIALIZE + e.getMessage());
    }
    return skipAmount;
  }

  private void writeLocationsToS3(String hatName, PdaReadResponse response, LambdaLogger logger) {
    try {
      String readResponse = MAPPER.writeValueAsString(response);
      RawLocationHistory locationHistory = MAPPER.readValue(readResponse, RawLocationHistory.class);
      Set<TemporalLocation> locations = locationHistory.getRawHistory()
          .stream()
          .map(RawTemporalLocation::getData)
          .collect(Collectors.toSet());

      LocationHistory history = LocationHistory.builder()
          .id(hatName)
          .addAllHistory(locations)
          .build();

      File file = new File(hatName);
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      writer.write(MAPPER.writeValueAsString(history));
      writer.close();
      writeObjectRequest(LOCATIONS_BUCKET, hatName, file, logger);
    } catch (IOException e) {
      logger.log(UNABLE_TO_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private void writeScoreToS3(String hatName, PdaReadResponse response, LambdaLogger logger) {
    try {
      String readResponse = MAPPER.writeValueAsString(response);
      RiskScore score = MAPPER.readValue(readResponse, RiskScore.class);
      File file = new File(hatName);
      BufferedWriter writer = new BufferedWriter(new FileWriter(file));
      writer.write(MAPPER.writeValueAsString(score));
      writer.close();
      writeObjectRequest(SCORE_BUCKET, hatName, file, logger);
    } catch (IOException e) {
      logger.log(UNABLE_TO_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private void writeObjectRequest(String bucket, String key, File file, LambdaLogger logger) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);
    PutObjectResult putObjectResult = S3_CLIENT.putObject(putObjectRequest);
    logger.log("PutObjectResult: \n" + putObjectResult.toString());
  }

  private String getScoreEndpoint(LambdaLogger logger) {
    String endpoint = null;
    try {
      endpoint = System.getenv(SCORE_ENDPOINT);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENDPOINT_MSG + e.getMessage());
      System.exit(1);
    }
    return endpoint;
  }

  private String getScoreNamespace(LambdaLogger logger) {
    String namespace = null;
    try {
      namespace = System.getenv(SCORE_NAMESPACE);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_NAMESPACE_MSG + e.getMessage());
      System.exit(1);
    }
    return namespace;
  }
}
