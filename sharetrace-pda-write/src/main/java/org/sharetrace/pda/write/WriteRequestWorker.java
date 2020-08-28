package org.sharetrace.pda.write;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import java.io.IOException;
import java.util.List;
import org.sharetrace.lambda.common.util.HandlerUtil;
import org.sharetrace.model.pda.request.AbstractPdaRequestUrl.Operation;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequest;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequestBody;
import org.sharetrace.model.pda.request.PdaRequestUrl;
import org.sharetrace.model.score.RiskScore;
import org.sharetrace.pda.common.ContractedPdaClient;

/**
 * Worker function that sends a write request to a contracted PDA.
 */
public class WriteRequestWorker implements
    RequestHandler<List<ContractedPdaWriteRequestBody<RiskScore>>, String> {

  // Logging messages
  private static final String CANNOT_FIND_ENV_VAR_MSG = "Unable to environment variable: \n";
  private static final String CANNOT_WRITE_TO_PDA_MSG = "Unable to write data to PDA: \n";

  // Environment variable keys
  private static final String SCORE_ENDPOINT = "scoreEndpoint";
  private static final String SCORE_NAMESPACE = "scoreNamespace";
  private static final String IS_SANDBOX = "isSandbox";

  private static final ContractedPdaClient PDA_CLIENT = new ContractedPdaClient();

  private LambdaLogger logger;

  @Override
  public String handleRequest(List<ContractedPdaWriteRequestBody<RiskScore>> input,
      Context context) {
    HandlerUtil.logEnvironment(input, context);
    logger = context.getLogger();
    input.forEach(this::handleRequest);
    return HandlerUtil.get200Ok();
  }

  private void handleRequest(ContractedPdaWriteRequestBody<RiskScore> input) {
    ContractedPdaWriteRequest<RiskScore> request = ContractedPdaWriteRequest.<RiskScore>builder()
        .pdaRequestUrl(getPdaRequestUrl())
        .writeRequestBody(input)
        .build();
    try {
      PDA_CLIENT.write(request);
    } catch (IOException e) {
      logger.log(CANNOT_WRITE_TO_PDA_MSG + e.getMessage());
    }
  }

  private PdaRequestUrl getPdaRequestUrl() {
    PdaRequestUrl url = null;
    try {
      url = PdaRequestUrl.builder()
          .operation(Operation.CREATE)
          .contracted(true)
          .sandbox(Boolean.parseBoolean(System.getenv(IS_SANDBOX)))
          .namespace(HandlerUtil.getEnvironmentVariable(SCORE_NAMESPACE))
          .endpoint(HandlerUtil.getEnvironmentVariable(SCORE_ENDPOINT))
          .build();
    } catch (NullPointerException e) {
      logger.log(CANNOT_FIND_ENV_VAR_MSG + e.getMessage());
      System.exit(1);
    }
    return url;
  }
}
