package org.sharetrace.pda;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.ShortLivedTokenRequest;
import org.sharetrace.model.pda.response.ShortLivedTokenResponse;
import org.sharetrace.model.util.ShareTraceUtil;

public class VentilatorRequestHandler {

  // Logging messages
  private static final String MISSING_TOKEN_MSG = "Long lived token is missing: \n";
  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";
  private static final String INCOMPLETE_REQUEST_MSG =
      "Failed to complete short-lived token request: \n";
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to find environment variable: \n";
  private static final String FAILED_TO_SERIALIZE_MSG =
      "Failed to serialize request body: \n";
  private static final String NO_WORKERS_MSG = "No worker functions were found. Exiting handler.";

  // TODO Finalize
  // Environment variable keys
  private static final String LONG_LIVED_TOKEN = "longLivedToken";
  private static final String CONTRACTS_SERVER_URL = "contractsServerUrl";
  private static final String CONTRACT_ID = "contractId";

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private final AWSLambdaAsync lambdaClient;

  private final LambdaLogger logger;

  private final List<String> lambdaWorkerKeys;

  private final int partitionSize;

  public VentilatorRequestHandler(AWSLambdaAsync lambdaClient, LambdaLogger logger,
      List<String> lambdaWorkerKeys, int partitionSize) {
    this.logger = logger;
    this.lambdaClient = lambdaClient;
    this.lambdaWorkerKeys = lambdaWorkerKeys;
    this.partitionSize = partitionSize;
  }

  public void handleRequest() {
    ShortLivedTokenRequest tokenRequest = ShortLivedTokenRequest.builder()
        .longLivedToken(getLongLivedToken())
        .contractsServerUrl(getContractsServerUrl())
        .build();
    ShortLivedTokenResponse tokenResponse = getShortLivedTokenResponse(tokenRequest);

    Optional<List<String>> hatsOptional = tokenResponse.getData();
    Optional<String> shortLivedTokenOptional = tokenResponse.getShortLivedToken();
    Optional<String> errorOptional = tokenResponse.getError();
    Optional<String> messageOptional = tokenResponse.getMessage();
    Optional<String> causeOptional = tokenResponse.getCause();

    if (hatsOptional.isPresent() && shortLivedTokenOptional.isPresent()) {
      invokeWorkers(hatsOptional.get(), shortLivedTokenOptional.get());
    } else if (errorOptional.isPresent()) {
      if (messageOptional.isPresent()) {
        logger.log(errorOptional.get() + "\n" + messageOptional.get());
        System.exit(1);
      } else if (causeOptional.isPresent()) {
        logger.log(errorOptional.get() + "\n" + causeOptional.get());
        System.exit(1);
      }
    } else {
      logger.log(tokenResponse.getData().toString());
      System.exit(1);
    }
  }

  private String getLongLivedToken() {
    String longLivedToken = null;
    try {
      longLivedToken = System.getenv(LONG_LIVED_TOKEN);
    } catch (NullPointerException e) {
      logger.log(MISSING_TOKEN_MSG + e.getMessage());
      System.exit(1);
    }
    return longLivedToken;
  }

  private URL getContractsServerUrl() {
    URL contractsServerUrl = null;
    try {
      contractsServerUrl = new URL(System.getenv(CONTRACTS_SERVER_URL));
    } catch (MalformedURLException e) {
      logger.log(MALFORMED_URL_MSG + e.getMessage());
      System.exit(1);
    }
    return contractsServerUrl;
  }

  private ShortLivedTokenResponse getShortLivedTokenResponse(ShortLivedTokenRequest request) {
    ShortLivedTokenResponse tokenResponse = null;
    try {
      tokenResponse = PDA_CLIENT.getShortLivedToken(request);
    } catch (IOException e) {
      logger.log(INCOMPLETE_REQUEST_MSG + e.getMessage());
      System.exit(1);
    }
    return tokenResponse;
  }

  private void invokeWorkers(List<String> hats, String shortLivedToken) {
    double nHats = hats.size();
    int nPartitions = (int) Math.ceil(nHats / partitionSize);
    List<String> lambdaFunctionNames = getLambdaFunctionNames(lambdaWorkerKeys);
    if (lambdaFunctionNames.isEmpty()) {
      logger.log(NO_WORKERS_MSG);
      System.exit(1);
    }
    int nWorkers = lambdaFunctionNames.size();
    String contractId = getContractId();

    for (int iPartition = 0; iPartition < nPartitions; iPartition++) {
      int startIndex = iPartition * partitionSize;
      int endIndex = (iPartition + 1) * partitionSize - 1;
      List<ContractedPdaRequestBody> requestBodies = IntStream.range(startIndex, endIndex)
          .mapToObj(hats::get)
          .map(hat -> ContractedPdaRequestBody.builder()
              .contractId(contractId)
              .hatName(hat)
              .shortLivedToken(shortLivedToken)
              .build())
          .collect(Collectors.toList());
      int iWorker = iPartition % nWorkers;

      try {
        InvokeRequest invokeRequest = new InvokeRequest()
            .withFunctionName(lambdaFunctionNames.get(iWorker))
            .withInvocationType(InvocationType.Event)
            .withPayload(MAPPER.writeValueAsString(requestBodies));
        lambdaClient.invokeAsync(invokeRequest);
      } catch (JsonProcessingException e) {
        logger.log(FAILED_TO_SERIALIZE_MSG + requestBodies.toString());
      }
    }
  }

  private String getContractId() {
    String contractId = null;
    try {
      contractId = System.getenv(CONTRACT_ID);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return contractId;
  }

  public List<String> getLambdaFunctionNames(List<String> environmentVariableKeys) {
    return environmentVariableKeys.stream()
        .map(this::getFunctionName)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private String getFunctionName(String environmentVariableKey) {
    String functionName = null;
    try {
      functionName = System.getenv(environmentVariableKey);
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
    }
    return functionName;
  }
}
