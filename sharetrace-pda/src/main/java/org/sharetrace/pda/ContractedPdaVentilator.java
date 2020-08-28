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
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.sharetrace.model.pda.request.ShortLivedTokenRequest;
import org.sharetrace.model.pda.response.ShortLivedTokenResponse;
import org.sharetrace.model.util.ShareTraceUtil;
import org.sharetrace.pda.util.HandlerUtil;

/**
 * Provides the common functionality of ventilator function that interacts with contracted PDAs.
 *
 * @param <T> Type of the payload processed by a worker function.
 */
public abstract class ContractedPdaVentilator<T> implements Ventilator<T> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String MALFORMED_URL_MSG = "Malformed contracts server URL: \n";
  private static final String INCOMPLETE_REQUEST_MSG = "Failed to complete short-lived token request: \n";
  private static final String FAILED_TO_SERIALIZE_MSG = "Failed to serialize request body: \n";
  private static final String NO_WORKERS_MSG = "No worker functions were found. Exiting handler";

  // Environment variable keys
  private static final String LONG_LIVED_TOKEN = "longLivedToken";
  private static final String CONTRACTS_SERVER_URL = "contractsServerUrl";
  private static final String CONTRACT_ID = "contractId";

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private final AWSLambdaAsync lambdaClient;

  private final List<String> lambdaWorkerKeys;

  private final int partitionSize;

  private LambdaLogger logger;

  public ContractedPdaVentilator(AWSLambdaAsync lambdaClient, LambdaLogger logger,
      List<String> lambdaWorkerKeys, int partitionSize) {
    this.logger = logger;
    this.lambdaClient = lambdaClient;
    this.lambdaWorkerKeys = lambdaWorkerKeys;
    this.partitionSize = partitionSize;
  }

  @Override
  public void handleRequest() {
    ShortLivedTokenRequest tokenRequest = ShortLivedTokenRequest.builder()
        .longLivedToken(getEnvironmentVariable(LONG_LIVED_TOKEN))
        .contractsServerUrl(getContractsServerUrl())
        .build();
    ShortLivedTokenResponse response = getShortLivedToken(tokenRequest);

    Optional<List<String>> hats = response.getData();
    Optional<String> shortLivedToken = response.getShortLivedToken();
    Optional<String> error = response.getError();
    Optional<String> cause = response.getCause();

    if (hats.isPresent() && shortLivedToken.isPresent()) {
      invokeWorkers(hats.get(), shortLivedToken.get());
    } else if (error.isPresent() && cause.isPresent()) {
      log(error.get() + "\n" + cause.get());
      System.exit(1);
    } else {
      log(response.getData().toString());
      System.exit(1);
    }
  }

  private URL getContractsServerUrl() {
    URL contractsServerUrl = null;
    try {
      contractsServerUrl = new URL(getEnvironmentVariable(CONTRACTS_SERVER_URL));
    } catch (MalformedURLException e) {
      log(MALFORMED_URL_MSG + e.getMessage());
      System.exit(1);
    }
    return contractsServerUrl;
  }

  private ShortLivedTokenResponse getShortLivedToken(ShortLivedTokenRequest request) {
    ShortLivedTokenResponse tokenResponse = null;
    try {
      tokenResponse = PDA_CLIENT.getShortLivedToken(request);
    } catch (IOException e) {
      log(INCOMPLETE_REQUEST_MSG + e.getMessage());
      System.exit(1);
    }
    return tokenResponse;
  }

  private void invokeWorkers(List<String> hats, String shortLivedToken) {
    List<String> workers = getWorkers();
    if (workers.isEmpty()) {
      log(NO_WORKERS_MSG);
      System.exit(1);
    }
    double nHats = hats.size();
    int nPartitions = (int) Math.ceil(nHats / partitionSize);
    int nWorkers = workers.size();
    for (int iPartition = 0; iPartition < nPartitions; iPartition++) {
      int startIndex = iPartition * partitionSize;
      int endIndex = (iPartition + 1) * partitionSize - 1;
      Set<T> payload = IntStream.range(startIndex, endIndex)
          .mapToObj(hats::get)
          .map(hat -> mapToPayload(hat, shortLivedToken))
          .filter(Objects::nonNull)
          .collect(Collectors.toSet());
      int iWorker = iPartition % nWorkers;
      invokeWorker(workers.get(iWorker), payload);
    }
  }

  @Override
  public List<String> getWorkers() {
    return lambdaWorkerKeys.stream().map(this::getEnvironmentVariable).collect(Collectors.toList());
  }

  abstract T mapToPayload(String hat, String shortLivedToken);

  @Override
  public void invokeWorker(String worker, Collection<T> payload) {
    try {
      InvokeRequest invokeRequest = new InvokeRequest()
          .withFunctionName(worker)
          .withInvocationType(InvocationType.Event)
          .withPayload(MAPPER.writeValueAsString(payload));
      lambdaClient.invokeAsync(invokeRequest);
    } catch (JsonProcessingException e) {
      log(FAILED_TO_SERIALIZE_MSG + payload.toString());
    }
  }

  String getContractId() {
    return getEnvironmentVariable(CONTRACT_ID);
  }

  String getEnvironmentVariable(String key) {
    String value = null;
    try {
      value = HandlerUtil.getEnvironmentVariable(key);
    } catch (NullPointerException e) {
      log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
    }
    return value;
  }

  private void log(String message) {
    if (logger != null) {
      logger.log(message);
    }
  }

  LambdaLogger getLogger() {
    return logger;
  }

  void setLogger(LambdaLogger logger) {
    this.logger = logger;
  }
}
