package org.sharetrace.pda.common;

import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.model.InvocationType;
import com.amazonaws.services.lambda.model.InvokeRequest;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
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
import org.sharetrace.lambda.common.Ventilator;
import org.sharetrace.lambda.common.util.HandlerUtil;
import org.sharetrace.model.pda.request.ShortLivedTokenRequest;
import org.sharetrace.model.pda.response.ShortLivedTokenResponse;
import org.sharetrace.model.util.ShareTraceUtil;

/**
 * Provides the common functionality of ventilator function that interacts with contracted PDAs.
 *
 * @param <T> Type of the payload processed by a worker function.
 */
public abstract class ContractedPdaVentilator<T> implements Ventilator<T> {

  // Logging messages
  private static final String MALFORMED_URL_MSG = HandlerUtil.getMalformedUrlMsg();
  private static final String INCOMPLETE_REQUEST_MSG = HandlerUtil.getIncompleteRequestMsg();
  private static final String NO_WORKERS_MSG = HandlerUtil.getNoWorkersMsg();
  private static final String FAILED_TO_DESERIALIZE_MSG = HandlerUtil.getFailedToSerializeMsg();
  private static final String CANNOT_FIND_ENV_VAR_MSG = HandlerUtil.getCannotFindEnvVarMsg();

  // Environment variable keys
  private static final String LONG_LIVED_TOKEN = "longLivedToken";
  private static final String CONTRACTS_SERVER_URL = "contractsServerUrl";
  private static final String CONTRACT_ID = "contractId";

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private final AWSLambdaAsync lambdaClient;

  private final List<String> workerKeys;

  private final int partitionSize;

  private LambdaLogger logger;

  public ContractedPdaVentilator(AWSLambdaAsync lambdaClient, LambdaLogger logger,
      List<String> lambdaWorkerKeys, int partitionSize) {
    this.logger = logger;
    this.lambdaClient = lambdaClient;
    this.workerKeys = lambdaWorkerKeys;
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
      logMessage(error.get() + "\n" + cause.get());
      System.exit(1);
    } else {
      logMessage(response.getData().toString());
      System.exit(1);
    }
  }

  private URL getContractsServerUrl() {
    URL contractsServerUrl = null;
    try {
      contractsServerUrl = new URL(getEnvironmentVariable(CONTRACTS_SERVER_URL));
    } catch (MalformedURLException e) {
      logException(e, MALFORMED_URL_MSG);
      System.exit(1);
    }
    return contractsServerUrl;
  }

  private ShortLivedTokenResponse getShortLivedToken(ShortLivedTokenRequest request) {
    ShortLivedTokenResponse tokenResponse = null;
    try {
      tokenResponse = PDA_CLIENT.getShortLivedToken(request);
    } catch (IOException e) {
      logException(e, INCOMPLETE_REQUEST_MSG);
      System.exit(1);
    }
    return tokenResponse;
  }

  private void invokeWorkers(List<String> hats, String shortLivedToken) {
    List<String> workers = getWorkers();
    if (workers.isEmpty()) {
      logMessage(NO_WORKERS_MSG);
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
    return ImmutableList.copyOf(workerKeys.stream()
        .map(this::getEnvironmentVariable)
        .collect(Collectors.toList()));
  }

  protected abstract T mapToPayload(String hat, String shortLivedToken);

  @Override
  public void invokeWorker(String worker, Collection<T> payload) {
    try {
      InvokeRequest invokeRequest = new InvokeRequest()
          .withFunctionName(worker)
          .withInvocationType(InvocationType.Event)
          .withPayload(MAPPER.writeValueAsString(payload));
      lambdaClient.invokeAsync(invokeRequest);
    } catch (JsonProcessingException e) {
      logException(e, FAILED_TO_DESERIALIZE_MSG);
    }
  }

  protected String getContractId() {
    return getEnvironmentVariable(CONTRACT_ID);
  }

  String getEnvironmentVariable(String key) {
    String value = null;
    try {
      value = HandlerUtil.getEnvironmentVariable(key);
    } catch (NullPointerException e) {
      logException(e, CANNOT_FIND_ENV_VAR_MSG);
    }
    return value;
  }

  private void logMessage(String message) {
    if (logger != null) {
      HandlerUtil.logMessage(logger, message);
    }
  }

  private void logException(Exception e, String message) {
    if (logger != null) {
      HandlerUtil.logException(logger, e, message);
    }
  }

  protected LambdaLogger getLogger() {
    return logger;
  }

  protected void setLogger(LambdaLogger logger) {
    this.logger = logger;
  }
}