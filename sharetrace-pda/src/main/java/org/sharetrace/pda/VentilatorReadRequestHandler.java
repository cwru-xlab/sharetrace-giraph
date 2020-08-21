package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import org.sharetrace.pda.util.LambdaHandlerLogging;

/**
 * Retrieves the short-lived token and list associated HATs to retrieve data from each PDA.
 * <p>
 * This Lambda function attempts to execute one or more worker Lambda functions that execute the
 * read requests and store the data, if successful, in S3.
 */
public class VentilatorReadRequestHandler implements RequestHandler<ScheduledEvent, String> {

  // Logging messages
  private static final String MISSING_TOKEN_MSG = "Long lived token is missing: \n";
  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";
  private static final String INCOMPLETE_REQUEST_MSG =
      "Failed to complete short-lived token request: \n";
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to find environment variable: \n";
  private static final String FAILED_TO_SERIALIZE_MSG =
      "Failed to serialize request body: \n";

  // Environment variable keys
  private static final String LONG_LIVED_TOKEN = "longLivedToken";
  private static final String CONTRACTS_SERVER_URL = "contractsServerUrl";
  private static final String FIRST_WORKER_LAMBDA = "lambdaWorker1";
  private static final String SECOND_WORKER_LAMBDA = "lambdaWorker2";
  private static final String CONTRACT_ID = "contractId";

  // Clients
  private static final AWSLambdaAsync LAMBDA_CLIENT = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();
  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final int PARTITION_SIZE = 50;

  @Override
  public String handleRequest(ScheduledEvent input, Context context) {
    LambdaHandlerLogging.logEnvironment(input, context);
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
              .withInvocationType(InvocationType.Event)
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
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return ImmutableList.of(firstFunction, secondFunction);
  }

  private String getContractId(LambdaLogger logger) {
    String contractId = null;
    try {
      contractId = System.getenv(CONTRACT_ID);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return contractId;
  }
}
