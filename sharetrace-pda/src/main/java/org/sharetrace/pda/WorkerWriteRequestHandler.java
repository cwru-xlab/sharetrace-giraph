package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import org.sharetrace.model.pda.request.AbstractPdaRequestUrl.Operation;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequest;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequestBody;
import org.sharetrace.model.pda.request.PdaRequestUrl;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.model.vertex.VariableVertex;
import org.sharetrace.pda.util.LambdaHandlerLogging;

public class WorkerWriteRequestHandler implements
    RequestHandler<List<ContractedPdaRequestBody>, String> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String CANNOT_DESERIALIZE = "Unable to deserialize: \n";
  private static final String CANNOT_WRITE_TO_PDA_MSG = "Unable to write data to PDA: \n";

  // TODO Finalize -- writing risk score back will use different namespace than reading
  // Environment variable keys
  private static final String SCORE_ENDPOINT = "scoreEndpoint";
  private static final String SCORE_NAMESPACE = "scoreNamespace";
  private static final String SCORE_BUCKET = "sharetrace-scores";
  private static final String IS_SANDBOX = "isSandbox";

  // Clients
  private static final AmazonS3 S3_CLIENT = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final String OUTPUT = "output";

  private static final String SCORE = "score";

  @Override
  public String handleRequest(List<ContractedPdaRequestBody> input, Context context) {
    LambdaHandlerLogging.logEnvironment(input, context);
    LambdaLogger logger = context.getLogger();
    input.forEach(entry -> handleRequest(entry, logger));
    return null;
  }

  private void handleRequest(ContractedPdaRequestBody input, LambdaLogger logger) {
    PdaRequestUrl url = getPdaRequestUrl(logger);
    String hatName = input.getHatName();
    double riskScore = getRiskScoreFromS3(hatName, logger);
    Map<String, Object> data = ImmutableMap.of(SCORE, riskScore);
    ContractedPdaWriteRequestBody body = ContractedPdaWriteRequestBody.builder()
        .contractId(input.getContractId())
        .hatName(hatName)
        .shortLivedToken(input.getShortLivedToken())
        .putAllData(data)
        .build();
    ContractedPdaWriteRequest request = ContractedPdaWriteRequest.builder()
        .pdaRequestUrl(url)
        .writeRequestBody(body)
        .build();
    try {
      PDA_CLIENT.writeToContractedPda(request);
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_PDA_MSG + e.getMessage());
    }
  }

  private PdaRequestUrl getPdaRequestUrl(LambdaLogger logger) {
    PdaRequestUrl url = null;
    try {
      String isSandbox = System.getenv(IS_SANDBOX);
      boolean sandbox = Boolean.parseBoolean(isSandbox);
      url = PdaRequestUrl.builder()
          .operation(Operation.CREATE)
          .sandbox(sandbox)
          .contracted(true)
          .namespace(SCORE_NAMESPACE)
          .endpoint(SCORE_ENDPOINT)
          .build();
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return url;
  }

  private double getRiskScoreFromS3(String hatName, LambdaLogger logger) {
    GetObjectRequest objectRequest = new GetObjectRequest(SCORE_BUCKET, OUTPUT);
    S3Object object = S3_CLIENT.getObject(objectRequest);
    S3ObjectInputStream input = object.getObjectContent();
    double score = 0.0;

    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8));
      Optional<Double> riskScore = reader.lines()
          .filter(isForHatName(hatName, logger))
          .map(line -> toRiskScore(line, logger))
          .findFirst();
      // New PDA should have initial risk score of 0.0
      score = riskScore.orElse(0.0);
      reader.close();
    } catch (IOException e) {
      logger.log(CANNOT_DESERIALIZE + e.getMessage());
    }
    return score;
  }

  private Predicate<String> isForHatName(String hatName, LambdaLogger logger) {
    return s -> {
      Optional<VariableVertex> vertex = Optional.empty();
      try {
        vertex = Optional.of(MAPPER.readValue(s, VariableVertex.class));
      } catch (JsonProcessingException e) {
        logger.log(CANNOT_DESERIALIZE + e.getMessage());
      }
      return vertex.map(v -> v.getVertexId().getIds().first().equals(hatName)).orElse(false);
    };
  }

  private double toRiskScore(String s, LambdaLogger logger) {
    double riskScore = 0.0;
    try {
      VariableVertex vertex = MAPPER.readValue(s, VariableVertex.class);
      riskScore = vertex.getVertexValue().getMessage().first().getValue();
    } catch (JsonProcessingException e) {
      logger.log(CANNOT_DESERIALIZE + e.getMessage());
    }
    return riskScore;
  }
}
