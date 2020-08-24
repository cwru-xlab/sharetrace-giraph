package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.location.TemporalLocation;
import org.sharetrace.model.pda.HatContext;
import org.sharetrace.model.pda.Payload;
import org.sharetrace.model.pda.request.AbstractPdaReadRequestParameters.Ordering;
import org.sharetrace.model.pda.request.AbstractPdaRequestUrl.Operation;
import org.sharetrace.model.pda.request.ContractedPdaReadRequest;
import org.sharetrace.model.pda.request.ContractedPdaReadRequestBody;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.PdaReadRequestParameters;
import org.sharetrace.model.pda.request.PdaRequestUrl;
import org.sharetrace.model.pda.response.PdaResponse;
import org.sharetrace.model.pda.response.Record;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.pda.util.LambdaHandlerLogging;

/**
 * Retrieves location and score data from a PDA. If successful, the data is stored in S3 for later
 * processing.
 * <p>
 * This Lambda function is invoked by {@link VentilatorReadRequestHandler}.
 */
public class WorkerReadRequestHandler<T> implements
    RequestHandler<List<ContractedPdaRequestBody>, String> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String CANNOT_DESERIALIZE = "Unable to deserialize: \n";
  private static final String CANNOT_WRITE_TO_S3_MSG = "Unable to write to S3: \n";
  private static final String CANNOT_READ_FROM_PDA_MSG = "Unable to read data from PDA: \n";

  // TODO Finalize
  // Environment variable keys
  private static final String LOCATIONS_ENDPOINT = "locationsEndpoint";
  private static final String LOCATIONS_NAMESPACE = "locationsNamespace";
  private static final String IS_SANDBOX = "isSandbox";
  private static final String HAT_CONTEXT_BUCKET = "sharetrace-hatContext";
  private static final String LOCATIONS_BUCKET = "sharetrace-locations";
  private static final String SCORE_ENDPOINT = "scoreEndpoint";
  private static final String SCORE_NAMESPACE = "scoreNamespace";
  private static final String SCORE_BUCKET = "sharetrace-scores";

  // Clients
  private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final String ORDER_BY = "timestamp";
  private static final String INPUT_SEGMENT = "input/";

  @Override
  public String handleRequest(List<ContractedPdaRequestBody> input, Context context) {
    LambdaHandlerLogging.logEnvironment(input, context);
    LambdaLogger logger = context.getLogger();
    input.forEach(entry -> handleRequest(entry, logger));
    return null;
  }

  private void handleRequest(ContractedPdaRequestBody input, LambdaLogger logger) {
    handleLocationsRequest(input, logger);
    handleScoreRequest(input, logger);
  }

  private void handleLocationsRequest(ContractedPdaRequestBody input, LambdaLogger logger) {
    String locsEndpoint = getLocationsEndpoint(logger);
    String locsNamespace = getLocationNamespace(logger);
    PdaRequestUrl.Builder builder = getCommonUrlBuilder(logger);
    PdaRequestUrl locsUrl = getPdaRequestUrl(builder, locsEndpoint, locsNamespace);
    String hatName = input.getHatName();
    int skipAmount = getSkipAmount(hatName, logger);
    PdaReadRequestParameters parameters = PdaReadRequestParameters.builder()
        .orderBy(ORDER_BY)
        .ordering(Ordering.ASCENDING)
        .skipAmount(skipAmount)
        .build();
    ContractedPdaReadRequestBody body = ContractedPdaReadRequestBody.builder()
        .baseRequestBody(input)
        .parameters(parameters)
        .build();
    ContractedPdaReadRequest locationsRequest = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(locsUrl)
        .readRequestBody(body)
        .build();
    PdaResponse<TemporalLocation> locationsResponse = getLocationResponse(locationsRequest, logger);
    int nLocationsWritten = writeLocationsToS3(locationsResponse, hatName, logger);
    HatContext updatedContext = HatContext.builder()
        .hatName(hatName)
        .numRecordsRead(nLocationsWritten + skipAmount)
        .build();
    updateHatContext(updatedContext, logger);
  }

  private String getLocationsEndpoint(LambdaLogger logger) {
    String endpoint = null;
    try {
      endpoint = System.getenv(LOCATIONS_ENDPOINT);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return endpoint;
  }

  private String getLocationNamespace(LambdaLogger logger) {
    String namespace = null;
    try {
      namespace = System.getenv(LOCATIONS_NAMESPACE);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return namespace;
  }

  private int getSkipAmount(String hatName, LambdaLogger logger) {
    GetObjectRequest objectRequest = new GetObjectRequest(HAT_CONTEXT_BUCKET, getKey(hatName));
    S3Object object = S3_CLIENT.getObject(objectRequest);
    S3ObjectInputStream input = object.getObjectContent();
    int skipAmount = 0;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8))) {
      HatContext hatContext = MAPPER.readValue(reader.readLine(), HatContext.class);
      skipAmount = hatContext.getNumRecordsRead();
    } catch (IOException e) {
      logger.log(CANNOT_DESERIALIZE + e.getMessage());
    }
    return skipAmount;
  }

  private String getKey(String hatName) {
    return INPUT_SEGMENT + hatName;
  }

  private int writeLocationsToS3(PdaResponse<TemporalLocation> response, String hatName,
      LambdaLogger logger) {
    File file = new File(getKey(hatName));
    int nLocationsWritten = 0;
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      Optional<List<Record<TemporalLocation>>> records = response.getData();
      if (records.isPresent() && records.get().size() > 0) {
        LocationHistory history = transform(records.get(), hatName);
        writer.write(MAPPER.writeValueAsString(history));
        writeObjectRequest(LOCATIONS_BUCKET, getKey(hatName), file);
        nLocationsWritten = history.getHistory().size();
      } else {
        logFailedResponse(response.getError(), response.getMessage(), response.getCause(), logger);
      }
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
    return nLocationsWritten;
  }

  private LocationHistory transform(List<Record<TemporalLocation>> records, String hatName) {
    Set<TemporalLocation> locations = records
        .stream()
        .map(Record::getPayload)
        .map(Payload::getData)
        .collect(Collectors.toSet());
    return LocationHistory.builder()
        .id(hatName)
        .addAllHistory(locations)
        .build();
  }

  private void logFailedResponse(Optional<String> error, Optional<String> message,
      Optional<String> cause, LambdaLogger logger) {
    if (error.isPresent()) {
      String err = error.get();
      if (message.isPresent()) {
        logger.log(CANNOT_WRITE_TO_S3_MSG + err + "\n" + message.get());
      } else if (cause.isPresent()) {
        logger.log(CANNOT_WRITE_TO_S3_MSG + err + "\n" + cause.get());
      } else {
        logger.log(CANNOT_WRITE_TO_S3_MSG + err);
      }
    }
  }

  private void updateHatContext(HatContext updatedContext, LambdaLogger logger) {
    String key = getKey(updatedContext.getHatName());
    File updated = new File(key);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(updated))) {
      writer.write(MAPPER.writeValueAsString(updatedContext));
      S3_CLIENT.putObject(HAT_CONTEXT_BUCKET, key, updated);
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private void handleScoreRequest(ContractedPdaRequestBody input, LambdaLogger logger) {
    String scoreEndpoint = getScoreEndpoint(logger);
    String scoreNamespace = getScoreNamespace(logger);
    PdaRequestUrl.Builder builder = getCommonUrlBuilder(logger);
    PdaRequestUrl scoreUrl = getPdaRequestUrl(builder, scoreEndpoint, scoreNamespace);
    ContractedPdaReadRequest scoreRequest = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(scoreUrl)
        .build();
    PdaResponse<RiskScore> scoreResponse = getRiskScoreResponse(scoreRequest, logger);
    writeScoreToS3(scoreResponse, input.getHatName(), logger);
  }

  private void writeScoreToS3(PdaResponse<RiskScore> response, String hatName,
      LambdaLogger logger) {
    String key = getKey(hatName);
    File file = new File(key);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file));) {
      Optional<List<Record<RiskScore>>> records = response.getData();
      if (records.isPresent() && records.get().size() > 0) {
        RiskScore riskScore = records.get().get(0).getPayload().getData();
        writer.write(MAPPER.writeValueAsString(riskScore));
        writeObjectRequest(SCORE_BUCKET, key, file);
      } else {
        logFailedResponse(response.getError(), response.getMessage(), response.getCause(), logger);
      }
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private void writeObjectRequest(String bucket, String key, File file) {
    PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);
    S3_CLIENT.putObject(putObjectRequest);
  }

  private String getScoreEndpoint(LambdaLogger logger) {
    String endpoint = null;
    try {
      endpoint = System.getenv(SCORE_ENDPOINT);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return endpoint;
  }

  private String getScoreNamespace(LambdaLogger logger) {
    String namespace = null;
    try {
      namespace = System.getenv(SCORE_NAMESPACE);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return namespace;
  }

  private PdaRequestUrl.Builder getCommonUrlBuilder(LambdaLogger logger) {
    PdaRequestUrl.Builder commonUrlBuilder = null;
    try {
      String isSandbox = System.getenv(IS_SANDBOX);
      boolean sandbox = Boolean.parseBoolean(isSandbox);
      commonUrlBuilder = PdaRequestUrl.builder()
          .contracted(true)
          .operation(Operation.READ)
          .sandbox(sandbox);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return commonUrlBuilder;
  }

  private PdaRequestUrl getPdaRequestUrl(PdaRequestUrl.Builder builder, String endpoint,
      String namespace) {
    return builder
        .endpoint(endpoint)
        .namespace(namespace)
        .build();
  }

  private PdaResponse<TemporalLocation> getLocationResponse(ContractedPdaReadRequest readRequest,
      LambdaLogger logger) {
    PdaResponse<TemporalLocation> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      logger.log(CANNOT_READ_FROM_PDA_MSG + e.getMessage());
      System.exit(1);
    }
    return response;
  }

  private PdaResponse<RiskScore> getRiskScoreResponse(ContractedPdaReadRequest readRequest,
      LambdaLogger logger) {
    PdaResponse<RiskScore> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      logger.log(CANNOT_READ_FROM_PDA_MSG + e.getMessage());
      System.exit(1);
    }
    return response;
  }
}
