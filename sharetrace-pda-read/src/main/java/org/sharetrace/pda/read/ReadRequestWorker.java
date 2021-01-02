package org.sharetrace.pda.read;

import com.amazonaws.AmazonServiceException;
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
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.sharetrace.lambda.common.util.HandlerUtil;
import org.sharetrace.model.identity.IdGroup;
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
import org.sharetrace.model.pda.response.Response;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.score.SendableRiskScores;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.model.vertex.VariableVertex;
import org.sharetrace.pda.common.ContractedPdaClient;

/**
 * Retrieves location and score data from a PDA. If successful, the data is stored in S3 for later
 * processing.
 * <p>
 * This Lambda function is invoked by {@link ReadRequestVentilator}.
 */
public class ReadRequestWorker implements RequestHandler<List<ContractedPdaRequestBody>, String> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = HandlerUtil.getCannotFindEnvVarMsg();
  private static final String CANNOT_DESERIALIZE = HandlerUtil.getCannotDeserializeMsg();
  private static final String CANNOT_WRITE_TO_S3_MSG = HandlerUtil.getCannotWriteToS3Msg();
  private static final String CANNOT_READ_FROM_PDA_MSG = HandlerUtil.getCannotReadFromPdaMsg();
  private static final String HAT_DOES_NOT_EXIST = HandlerUtil.getHatDoesNotExistMsg();

  // Environment variable keys
  private static final String IS_SANDBOX = "isSandbox";
  private static final String LOCATIONS_ENDPOINT = "locationsEndpoint";
  private static final String LOCATIONS_NAMESPACE = "locationsNamespace";
  private static final String SCORE_ENDPOINT = "scoreEndpoint";
  private static final String SCORE_NAMESPACE = "scoreNamespace";

  private static final String HAT_CONTEXT_BUCKET = "sharetrace-hatContext";
  private static final String LOCATIONS_BUCKET = "sharetrace-locations";
  private static final String SCORE_BUCKET = "sharetrace-input";
  private static final String SCORE_PREFIX = "score-";
  private static final String LOCATION_PREFIX = "location-";
  private static final String FILE_FORMAT = ".txt";

  private static final int GEOHASH_OBFUSCATION_INDEX = 3;
  private static final String ORDER_BY = "timestamp";

  private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private LambdaLogger logger;

  @Override
  public String handleRequest(List<ContractedPdaRequestBody> input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    logger = context.getLogger();
    handleLocationsRequest(input);
    handleScoreRequest(input);
    return HandlerUtil.get200Ok();
  }

  private void handleLocationsRequest(Iterable<ContractedPdaRequestBody> input) {
    PdaRequestUrl url = getPdaRequestUrl(LOCATIONS_ENDPOINT, LOCATIONS_NAMESPACE);
    File file = createRandomTextFile(LOCATION_PREFIX);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      for (ContractedPdaRequestBody entry : input) {
        String hatName = entry.getHatName();
        int skipAmount = getSkipAmount(hatName);
        ContractedPdaReadRequest request = createLocationsRequest(entry, url, skipAmount);
        PdaResponse<TemporalLocation> response = getLocation(request);
        int nLocationsWritten = 0;
        if (response.isSuccess()) {
          LocationHistory history = transform(response, hatName);
          writer.write(MAPPER.writeValueAsString(history));
          writer.newLine();
          nLocationsWritten = history.getHistory().size();
        } else {
          logFailedResponse(response);
        }
        updateHatContext(hatName, nLocationsWritten + skipAmount);
      }
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, CANNOT_WRITE_TO_S3_MSG);
    }
    S3_CLIENT.putObject(LOCATIONS_BUCKET, file.getName(), file);
  }

  private File createRandomTextFile(String prefix) {
    return HandlerUtil.createRandomFile(prefix, FILE_FORMAT);
  }

  private String formatKey(String value) {
    return value + FILE_FORMAT;
  }

  private ContractedPdaReadRequest createLocationsRequest(ContractedPdaRequestBody body,
      PdaRequestUrl url, int skipAmount) {
    PdaReadRequestParameters parameters = PdaReadRequestParameters.builder()
        .orderBy(ORDER_BY)
        .ordering(Ordering.ASCENDING)
        .skipAmount(skipAmount)
        .build();
    ContractedPdaReadRequestBody readBody = ContractedPdaReadRequestBody.builder()
        .body(body)
        .parameters(parameters)
        .build();
    return ContractedPdaReadRequest.builder().url(url).readBody(readBody).build();
  }

  private PdaRequestUrl getPdaRequestUrl(String endpointKey, String namespaceKey) {
    return PdaRequestUrl.builder()
        .contracted(true)
        .operation(Operation.READ)
        .sandbox(Boolean.parseBoolean(getEnvironmentVariable(IS_SANDBOX)))
        .endpoint(getEnvironmentVariable(endpointKey))
        .namespace(getEnvironmentVariable(namespaceKey))
        .build();
  }

  private int getSkipAmount(String hatName) {
    int skipAmount = 0;
    try {
      S3Object object = S3_CLIENT.getObject(HAT_CONTEXT_BUCKET, formatKey(hatName));
      skipAmount = getNumRecordsRead(object.getObjectContent());
    } catch (AmazonServiceException e) {
      HandlerUtil.logException(logger, e, HAT_DOES_NOT_EXIST);
    }
    return skipAmount;
  }

  private int getNumRecordsRead(S3ObjectInputStream input) {
    int nRecordsRead = 0;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8))) {
      HatContext hatContext = MAPPER.readValue(reader.readLine(), HatContext.class);
      nRecordsRead = hatContext.getNumRecordsRead();
      input.close();
    } catch (IOException e) {
      input.abort();
      HandlerUtil.logException(logger, e, CANNOT_DESERIALIZE);
    }
    return nRecordsRead;
  }

  private PdaResponse<TemporalLocation> getLocation(ContractedPdaReadRequest readRequest) {
    PdaResponse<TemporalLocation> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, CANNOT_READ_FROM_PDA_MSG);
      System.exit(1);
    }
    return response;
  }

  private LocationHistory transform(PdaResponse<TemporalLocation> response, String hatName) {
    Set<TemporalLocation> locations = response.getData().get().stream()
        .map(Record::getPayload)
        .map(Payload::getData)
        .map(this::obfuscate)
        .collect(Collectors.toSet());
    return LocationHistory.builder()
        .id(hatName)
        .addAllHistory(locations)
        .build();
  }

  private TemporalLocation obfuscate(TemporalLocation location) {
    int endIndex = location.getLocation().length() - GEOHASH_OBFUSCATION_INDEX;
    return location.withLocation(location.getLocation().substring(0, endIndex));
  }

  private void logFailedResponse(Response<?> response) {
    if (response.isError()) {
      String error = response.getError().get();
      String cause = response.getCause().get();
      String msg = CANNOT_WRITE_TO_S3_MSG + "\n" + error + "\n" + cause;
      HandlerUtil.logMessage(logger, msg);
    } else {
      HandlerUtil.logMessage(logger, CANNOT_WRITE_TO_S3_MSG);
    }
  }

  private void updateHatContext(String hatName, int nRecordsRead) {
    File file = new File(formatKey(hatName));
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      HatContext context = HatContext.builder()
          .hatName(hatName)
          .numRecordsRead(nRecordsRead)
          .build();
      writer.write(MAPPER.writeValueAsString(context));
      S3_CLIENT.putObject(HAT_CONTEXT_BUCKET, file.getName(), file);
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, CANNOT_WRITE_TO_S3_MSG);
    }
  }

  private void handleScoreRequest(List<ContractedPdaRequestBody> input) {
    PdaRequestUrl url = getPdaRequestUrl(SCORE_ENDPOINT, SCORE_NAMESPACE);
    File file = createRandomTextFile(SCORE_PREFIX);
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
      for (ContractedPdaRequestBody entry : input) {
        ContractedPdaReadRequest request = createScoreRequest(entry, url);
        PdaResponse<RiskScore> response = getRiskScore(request);
        if (response.isSuccess()) {
          VariableVertex vertex = transform(response);
          writer.write(MAPPER.writeValueAsString(vertex));
          writer.newLine();
        } else {
          logFailedResponse(response);
        }
      }
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, CANNOT_WRITE_TO_S3_MSG);
    }
    S3_CLIENT.putObject(SCORE_BUCKET, file.getName(), file);
  }

  private ContractedPdaReadRequest createScoreRequest(ContractedPdaRequestBody body,
      PdaRequestUrl url) {
    PdaReadRequestParameters parameters = PdaReadRequestParameters.builder()
        .orderBy(ORDER_BY)
        .ordering(Ordering.DESCENDING)
        .takeAmount(1)
        .build();
    ContractedPdaReadRequestBody readBody = ContractedPdaReadRequestBody.builder()
        .body(body)
        .parameters(parameters)
        .build();
    return ContractedPdaReadRequest.builder().url(url).readBody(readBody).build();
  }

  private PdaResponse<RiskScore> getRiskScore(ContractedPdaReadRequest readRequest) {
    PdaResponse<RiskScore> response = null;
    try {
      response = PDA_CLIENT.read(readRequest);
    } catch (IOException e) {
      HandlerUtil.logException(logger, e, CANNOT_READ_FROM_PDA_MSG);
      System.exit(1);
    }
    return response;
  }

  private VariableVertex transform(PdaResponse<RiskScore> response) {
    RiskScore message = response.getData().get().get(0).getPayload().getData();
    String id = message.getId();
    return VariableVertex.builder()
        .vertexId(IdGroup.builder().addId(id).build())
        .vertexValue(SendableRiskScores.builder().addSender(id).addMessage(message).build())
        .build();
  }

  private String getEnvironmentVariable(String key) {
    String value = null;
    try {
      value = System.getenv(key);
    } catch (NullPointerException e) {
      HandlerUtil.logException(logger, e, CANNOT_FIND_ENV_VAR_MSG);
      System.exit(1);
    }
    return value;
  }
}
