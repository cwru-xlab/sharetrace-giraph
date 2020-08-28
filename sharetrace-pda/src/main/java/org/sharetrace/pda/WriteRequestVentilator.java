package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.sharetrace.model.pda.Payload;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequestBody;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.pda.util.HandlerUtil;

/**
 * This Lambda function attempts to execute one or more worker Lambda functions that execute the
 * write requests to contracted PDAs.
 * <p>
 * This function transforms each line in an output text file to a risk score. These scores are then
 * sent as the payload for a write request to a PDA. The worker functions execute the write
 * request.
 */
public class WriteRequestVentilator
    extends ContractedPdaVentilator<ContractedPdaWriteRequestBody<RiskScore>>
    implements RequestHandler<S3Event, String> {

  // Logging messages
  private static final String CANNOT_DESERIALIZE = "Unable to deserialize: \n";

  // Environment variable keys
  private static final String FIRST_WORKER_LAMBDA = "lambdaWriter1";
  private static final String SECOND_WORKER_LAMBDA = "lambdaWriter2";
  private static final List<String> WORKER_LAMBDAS =
      ImmutableList.of(FIRST_WORKER_LAMBDA, SECOND_WORKER_LAMBDA);

  private static final AWSLambdaAsync LAMBDA_CLIENT = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final AmazonS3 S3 = AmazonS3ClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final int PARTITION_SIZE = 50;

  private static final String OUTPUT_KEY = "output.txt";

  private Map<String, RiskScore> output;

  public WriteRequestVentilator() {
    super(LAMBDA_CLIENT, null, WORKER_LAMBDAS, PARTITION_SIZE);
  }

  @Override
  public String handleRequest(S3Event input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    setLogger(context.getLogger());
    S3EventNotificationRecord record = input.getRecords().get(0);
    String bucketName = record.getS3().getBucket().getName();
    S3Object object = S3.getObject(bucketName, OUTPUT_KEY);
    output = mapObject(object);
    handleRequest();
    return HandlerUtil.get200Ok();
  }

  private Map<String, RiskScore> mapObject(S3Object object) {
    Map<String, RiskScore> mapping = new HashMap<>();
    S3ObjectInputStream input = object.getObjectContent();
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(input, Charsets.UTF_8))) {
      mapping = reader.lines()
          .map(this::mapToRiskScore)
          .filter(Objects::nonNull)
          .collect(Collectors.toMap(RiskScore::getId, Function.identity()));
    } catch (IOException e) {
      getLogger().log(e.getMessage());
      getLogger().log(Arrays.toString(e.getStackTrace()));
    }
    return ImmutableMap.copyOf(mapping);
  }

  private RiskScore mapToRiskScore(String s) {
    RiskScore mapped = null;
    try {
      mapped = MAPPER.readValue(s, RiskScore.class);
    } catch (JsonProcessingException e) {
      getLogger().log(CANNOT_DESERIALIZE + e.getMessage());
      getLogger().log(Arrays.toString(e.getStackTrace()));
    }
    return mapped;
  }

  @Override
  public ContractedPdaWriteRequestBody<RiskScore> mapToPayload(String hat, String shortLivedToken) {
    ContractedPdaWriteRequestBody<RiskScore> requestBody = null;
    if (output.containsKey(hat)) {
      ContractedPdaRequestBody baseRequestBody = ContractedPdaRequestBody.builder()
          .hatName(hat)
          .contractId(getContractId())
          .shortLivedToken(shortLivedToken)
          .build();
      requestBody = ContractedPdaWriteRequestBody.<RiskScore>builder()
          .baseRequestBody(baseRequestBody)
          .payload(Payload.<RiskScore>builder().data(output.get(hat)).build())
          .build();
    }
    return requestBody;
  }
}
