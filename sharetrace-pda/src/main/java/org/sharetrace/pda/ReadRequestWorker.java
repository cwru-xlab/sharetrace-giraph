package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
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
import java.util.Collection;
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
import org.sharetrace.pda.util.HandlerUtil;

/**
 * Retrieves location and score data from a PDA. If successful, the data is stored in S3 for later
 * processing.
 * <p>
 * This Lambda function is invoked by {@link ReadRequestVentilator}.
 */
public class ReadRequestWorker implements
    RequestHandler<List<ContractedPdaRequestBody>, String> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String CANNOT_DESERIALIZE = "Unable to deserialize: \n";
  private static final String CANNOT_WRITE_TO_S3_MSG = "Unable to write to S3: \n";
  private static final String CANNOT_READ_FROM_PDA_MSG = "Unable to read data from PDA: \n";

  // Environment variable keys
  private static final String LOCATIONS_ENDPOINT = "locationsEndpoint";
  private static final String LOCATIONS_NAMESPACE = "locationsNamespace";
  private static final String IS_SANDBOX = "isSandbox";
  private static final String HAT_CONTEXT_BUCKET = "sharetrace-hatContext";
  private static final String LOCATIONS_BUCKET = "sharetrace-locations";
  private static final String SCORE_ENDPOINT = "scoreEndpoint";
  private static final String SCORE_NAMESPACE = "scoreNamespace";
  private static final String SCORE_BUCKET = "sharetrace-input";

  private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final String ORDER_BY = "timestamp";

  private LambdaLogger logger;

  // TODO Actually, want write all bodies to the SAME file, not individual files. For risk
  //  scores, it doesn't matter which partition it's written to. For geohashes, assuming the
  //  partitions are over written with each write, it doesn't matter. HOWEVER, when merging the
  //  contact data, we do need a way to at least retrieve which partition a contact belongs to.
  //  This could be done with a "lookup" file that the ventilator can refer to in order to decide
  //  the partition assignment
  @Override
  public String handleRequest(List<ContractedPdaRequestBody> input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    logger = context.getLogger();
    input.forEach(this::handleRequest);
    return HandlerUtil.get200Ok();
  }

  private void handleRequest(ContractedPdaRequestBody input) {
    handleLocationsRequest(input);
    handleScoreRequest(input);
  }

  private void handleLocationsRequest(ContractedPdaRequestBody input) {
    String endpoint = getEnvironmentVariable(LOCATIONS_ENDPOINT);
    String namespace = getEnvironmentVariable(LOCATIONS_NAMESPACE);
    PdaRequestUrl url = getPdaRequestUrl(endpoint, namespace);
    String hatName = input.getHatName();
    int skipAmount = getSkipAmount(hatName);
    PdaReadRequestParameters parameters = PdaReadRequestParameters.builder()
        .orderBy(ORDER_BY)
        .ordering(Ordering.ASCENDING)
        .skipAmount(skipAmount)
        .build();
    ContractedPdaReadRequestBody body = ContractedPdaReadRequestBody.builder()
        .baseRequestBody(input)
        .parameters(parameters)
        .build();
    ContractedPdaReadRequest request = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(url)
        .readRequestBody(body)
        .build();
    PdaResponse<TemporalLocation> response = getLocationResponse(request);
    int nLocationsWritten = writeLocationsToS3(response, hatName);
    HatContext updatedContext = HatContext.builder()
        .hatName(hatName)
        .numRecordsRead(nLocationsWritten + skipAmount)
        .build();
    updateHatContext(updatedContext);
  }

  private PdaRequestUrl getPdaRequestUrl(String endpoint, String namespace) {
    return PdaRequestUrl.builder()
        .contracted(true)
        .operation(Operation.READ)
        .sandbox(Boolean.parseBoolean(getEnvironmentVariable(IS_SANDBOX)))
        .endpoint(endpoint)
        .namespace(namespace)
        .build();
  }

  private int getSkipAmount(String hatName) {
    S3Object object = S3_CLIENT.getObject(HAT_CONTEXT_BUCKET, hatName);
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

  private PdaResponse<TemporalLocation> getLocationResponse(ContractedPdaReadRequest readRequest) {
    PdaResponse<TemporalLocation> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      logger.log(CANNOT_READ_FROM_PDA_MSG + e.getMessage());
      System.exit(1);
    }
    return response;
  }

  private int writeLocationsToS3(PdaResponse<TemporalLocation> response, String hatName) {
    int nLocationsWritten = 0;
    File file = new File(hatName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      Optional<List<Record<TemporalLocation>>> records = response.getData();
      if (records.isPresent() && !records.get().isEmpty()) {
        LocationHistory history = transform(records.get(), hatName);
        writer.write(MAPPER.writeValueAsString(history));
        S3_CLIENT.putObject(LOCATIONS_BUCKET, hatName, file);
        nLocationsWritten = history.getHistory().size();
      } else {
        logFailedResponse(response.getError(), response.getCause());
      }
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
    return nLocationsWritten;
  }

  private LocationHistory transform(Collection<Record<TemporalLocation>> records, String hatName) {
    Set<TemporalLocation> locations = records.stream()
        .map(Record::getPayload)
        .map(Payload::getData)
        .collect(Collectors.toSet());
    return LocationHistory.builder()
        .id(hatName)
        .addAllHistory(locations)
        .build();
  }

  private void logFailedResponse(Optional<String> error, Optional<String> cause) {
    if (error.isPresent() && cause.isPresent()) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + error.get() + "\n" + cause.get());
    }
  }

  private void updateHatContext(HatContext updatedContext) {
    String key = updatedContext.getHatName();
    File updated = new File(key);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(updated))) {
      writer.write(MAPPER.writeValueAsString(updatedContext));
      S3_CLIENT.putObject(HAT_CONTEXT_BUCKET, key, updated);
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private void handleScoreRequest(ContractedPdaRequestBody input) {
    String scoreEndpoint = getEnvironmentVariable(SCORE_ENDPOINT);
    String scoreNamespace = getEnvironmentVariable(SCORE_NAMESPACE);
    PdaRequestUrl scoreUrl = getPdaRequestUrl(scoreEndpoint, scoreNamespace);
    ContractedPdaReadRequest scoreRequest = ContractedPdaReadRequest.builder()
        .pdaRequestUrl(scoreUrl)
        .build();
    PdaResponse<RiskScore> scoreResponse = getRiskScoreResponse(scoreRequest);
    writeScoreToS3(scoreResponse, input.getHatName());
  }

  private void writeScoreToS3(PdaResponse<RiskScore> response, String hatName) {
    File file = new File(hatName);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file));) {
      Optional<List<Record<RiskScore>>> records = response.getData();
      if (records.isPresent() && !records.get().isEmpty()) {
        RiskScore riskScore = records.get().get(0).getPayload().getData();
        writer.write(MAPPER.writeValueAsString(riskScore));
        S3_CLIENT.putObject(SCORE_BUCKET, hatName, file);
      } else {
        logFailedResponse(response.getError(), response.getCause());
      }
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_S3_MSG + e.getMessage());
    }
  }

  private PdaResponse<RiskScore> getRiskScoreResponse(ContractedPdaReadRequest readRequest) {
    PdaResponse<RiskScore> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      logger.log(CANNOT_READ_FROM_PDA_MSG + e.getMessage());
      System.exit(1);
    }
    return response;
  }

  private String getEnvironmentVariable(String key) {
    String value = null;
    try {
      value = HandlerUtil.getEnvironmentVariable(key);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
    }
    return value;
  }
}
