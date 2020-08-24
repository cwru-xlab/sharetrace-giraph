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
import org.sharetrace.pda.util.HandlerUtil;

public class VentilatorRequestHandler {

  // Logging messages
  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";
  private static final String INCOMPLETE_REQUEST_MSG =
      "Failed to complete short-lived token request: \n";
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
        .longLivedToken(HandlerUtil.getEnvironmentVariable(LONG_LIVED_TOKEN, logger))
        .contractsServerUrl(getContractsServerUrl())
        .build();
    ShortLivedTokenResponse tokenResponse = getShortLivedTokenResponse(tokenRequest);

    Optional<List<String>> hats = tokenResponse.getData();
    Optional<String> shortLivedToken = tokenResponse.getShortLivedToken();
    Optional<String> error = tokenResponse.getError();
    Optional<String> cause = tokenResponse.getCause();

    if (hats.isPresent() && shortLivedToken.isPresent()) {
      invokeWorkers(hats.get(), shortLivedToken.get());
    } else if (error.isPresent() && cause.isPresent()) {
      logger.log(error.get() + "\n" + cause.get());
      System.exit(1);
    } else {
      logger.log(tokenResponse.getData().toString());
      System.exit(1);
    }
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
    String contractId = HandlerUtil.getEnvironmentVariable(CONTRACT_ID, logger);

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

  public List<String> getLambdaFunctionNames(List<String> environmentVariableKeys) {
    return environmentVariableKeys.stream()
        .map(k -> HandlerUtil.getEnvironmentVariable(k, logger))
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }
}
