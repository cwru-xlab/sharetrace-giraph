package org.sharetrace.pda.read;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.sharetrace.lambda.common.util.HandlerUtil;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.pda.common.ContractedPdaVentilator;

/**
 * Retrieves the short-lived token and list of associated HATs to retrieve data from each PDA.
 * <p>
 * This Lambda function attempts to execute one or more worker Lambda functions that execute the
 * read requests and store the data, if successful, in S3.
 */
public class ReadRequestVentilator extends ContractedPdaVentilator<ContractedPdaRequestBody>
    implements RequestHandler<ScheduledEvent, String> {

  private static final AWSLambdaAsync LAMBDA = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  // Environment variable keys
  private static final String FIRST_WORKER_LAMBDA = "lambdaReader1";
  private static final String SECOND_WORKER_LAMBDA = "lambdaReader2";
  private static final List<String> WORKER_LAMBDAS =
      ImmutableList.of(FIRST_WORKER_LAMBDA, SECOND_WORKER_LAMBDA);

  private static final int PARTITION_SIZE = 50;

  public ReadRequestVentilator() {
    super(LAMBDA, null, WORKER_LAMBDAS, PARTITION_SIZE);
  }

  @Override
  public String handleRequest(ScheduledEvent input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    setLogger(context.getLogger());
    handleRequest();
    return HandlerUtil.get200Ok();
  }

  @Override
  public ContractedPdaRequestBody mapToPayload(String hat, String shortLivedToken) {
    return ContractedPdaRequestBody.builder()
        .contractId(getContractId())
        .hatName(hat)
        .shortLivedToken(shortLivedToken)
        .build();
  }
}
