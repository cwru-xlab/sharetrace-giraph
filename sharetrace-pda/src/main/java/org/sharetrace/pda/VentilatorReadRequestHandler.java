package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.ShortLivedTokenRequest;
import org.sharetrace.model.pda.response.ShortLivedTokenResponse;
import org.sharetrace.model.util.ShareTraceUtil;

/**
 * Retrieves
 */
public class VentilatorReadRequestHandler implements RequestHandler<ScheduledEvent, String> {

  private static final String INVALID_INPUT_MSG = "Input must not be null";

  private static final String INVALID_CONTEXT_MSG = "Context must not be null";

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final String LONG_LIVED_TOKEN = "longLivedToken";

  private static final String MISSING_TOKEN_MSG = "Long lived token is missing: \n";

  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";

  private static final String INCOMPLETE_REQUEST_MSG =
      "Failed to complete short-lived token request: \n";

  private static final String CONTRACTS_SERVER_URL = "contractsServerUrl";

  private static final String FIRST_WORKER_LAMBDA = "lambdaWorker1";

  private static final String SECOND_WORKER_LAMBDA = "lambdaWorker2";

  private static final String CANNOT_FIND_LAMBDA_MSG =
      "Unable to find AWS Lambda function. Check the environment variables: \n";

  private static final String CONTRACT_ID = "contractId";

  private static final String CANNOT_FIND_CONTRACT_ID_MSG =
      "Failed to find contractId. Check the environment variables: \n";

  private static final String FAILED_TO_SERIALIZE_MSG =
      "Failed to serialize request body: \n";

  private static final int PARTITION_SIZE = 50;

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  private static final AWSLambdaAsync LAMBDA_CLIENT = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  @Override
  public String handleRequest(ScheduledEvent input, Context context) {
    Preconditions.checkNotNull(input, INVALID_INPUT_MSG);
    Preconditions.checkNotNull(context, INVALID_CONTEXT_MSG);
    log(input, context);
    LambdaLogger logger = context.getLogger();

    ShortLivedTokenRequest tokenRequest = ShortLivedTokenRequest.builder()
        .longLivedToken(getLongLivedToken(logger))
        .contractsServerUrl(getContractsServerUrl(logger))
        .build();
    ShortLivedTokenResponse tokenResponse = getShortLivedTokenResponse(tokenRequest, logger);

    Optional<List<String>> hatsOptional = tokenResponse.getHats();
    Optional<String> shortLivedTokenOptional = tokenResponse.getShortLivedToken();
    Optional<String> errorOptional = tokenResponse.getError();
    Optional<String> messageOptional = tokenResponse.getMessage();

    if (hatsOptional.isPresent() && shortLivedTokenOptional.isPresent()) {
      invokeWorkers(hatsOptional.get(), shortLivedTokenOptional.get(), logger);
    } else if (errorOptional.isPresent() && messageOptional.isPresent()) {
      logger.log(errorOptional.get() + "\n" + messageOptional.get());
      System.exit(1);
    } else {
      logger.log(tokenResponse.getEmpty().toString());
      System.exit(1);
    }
    return null;
  }

  private void log(ScheduledEvent input, Context context) {
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

  private String getLongLivedToken(LambdaLogger logger) {
    String longLivedToken = null;
    try {
      longLivedToken = System.getenv(LONG_LIVED_TOKEN);
    } catch (NullPointerException e) {
      logger.log(MISSING_TOKEN_MSG + e.getMessage());
      System.exit(1);
    }
    return longLivedToken;
  }

  private URL getContractsServerUrl(LambdaLogger logger) {
    URL contractsServerUrl = null;
    try {
      contractsServerUrl = new URL(System.getenv(CONTRACTS_SERVER_URL));
    } catch (MalformedURLException e) {
      logger.log(MALFORMED_URL_MSG + e.getMessage());
      System.exit(1);
    }
    return contractsServerUrl;
  }

  private ShortLivedTokenResponse getShortLivedTokenResponse(ShortLivedTokenRequest request,
      LambdaLogger logger) {
    ShortLivedTokenResponse tokenResponse = null;
    try {
      tokenResponse = PDA_CLIENT.getShortLivedToken(request);
    } catch (IOException e) {
      logger.log(INCOMPLETE_REQUEST_MSG + e.getMessage());
      System.exit(1);
    }
    return tokenResponse;
  }

  private void invokeWorkers(List<String> hats, String shortLivedToken, LambdaLogger logger) {
    double nAccounts = hats.size();
    int nPartitions = (int) Math.ceil(nAccounts / PARTITION_SIZE);
    List<String> workerNames = getLambdaFunctionNames(logger);
    String contractId = getContractId(logger);

    for (int iPartition = 0; iPartition < nPartitions; iPartition++) {
      int startIndex = iPartition * PARTITION_SIZE;
      int endIndex = (iPartition + 1) * PARTITION_SIZE - 1;
      int iWorker = iPartition % workerNames.size();

      for (int iHat = startIndex; iHat < endIndex; iHat++) {
        String hat = hats.get(iHat);
        ContractedPdaRequestBody requestBody = ContractedPdaRequestBody.builder()
            .contractId(contractId)
            .hatName(hat)
            .shortLivedToken(shortLivedToken)
            .build();

        try {
          InvokeRequest invokeRequest = new InvokeRequest()
              .withFunctionName(workerNames.get(iWorker))
              .withInvocationType(EVENT)
              .withPayload(MAPPER.writeValueAsString(requestBody));
          LAMBDA_CLIENT.invokeAsync(invokeRequest);
        } catch (JsonProcessingException e) {
          logger.log(FAILED_TO_SERIALIZE_MSG + requestBody.toString());
        }
      }
    }
  }

  private List<String> getLambdaFunctionNames(LambdaLogger logger) {
    String firstFunction = null;
    String secondFunction = null;
    try {
      firstFunction = System.getenv(FIRST_WORKER_LAMBDA);
      secondFunction = System.getenv(SECOND_WORKER_LAMBDA);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_LAMBDA_MSG + e.getMessage());
      System.exit(1);
    }
    return ImmutableList.of(firstFunction, secondFunction);
  }

  private String getContractId(LambdaLogger logger) {
    String contractId = null;
    try {
      contractId = System.getenv(CONTRACT_ID);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_CONTRACT_ID_MSG + e.getMessage());
      System.exit(1);
    }
    return contractId;
  }
}
