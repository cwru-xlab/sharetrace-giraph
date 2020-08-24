package org.sharetrace.pda;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.lambda.AWSLambdaAsync;
import com.amazonaws.services.lambda.AWSLambdaAsyncClientBuilder;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.ScheduledEvent;
import com.google.common.collect.ImmutableList;
import java.util.List;
import org.sharetrace.pda.util.HandlerUtil;

/**
 * Retrieves the short-lived token and list associated HATs to retrieve data from each PDA.
 * <p>
 * This Lambda function attempts to execute one or more worker Lambda functions that execute the
 * read requests and store the data, if successful, in S3.
 */
public class VentilatorReadRequestHandler implements RequestHandler<ScheduledEvent, String> {

  private static final AWSLambdaAsync LAMBDA_CLIENT = AWSLambdaAsyncClientBuilder.standard()
      .withRegion(Regions.US_EAST_2).build();

  // TODO Finalize
  // Environment variable keys
  private static final String FIRST_WORKER_LAMBDA = "lambdaReader1";
  private static final String SECOND_WORKER_LAMBDA = "lambdaReader2";
  private static final List<String> WORKER_LAMBDAS =
      ImmutableList.of(FIRST_WORKER_LAMBDA, SECOND_WORKER_LAMBDA);

  private static final int PARTITION_SIZE = 50;

  @Override
  public String handleRequest(ScheduledEvent input, Context context) {
    HandlerUtil.logEnvironment(input, context);
    LambdaLogger logger = context.getLogger();
    VentilatorRequestHandler handler =
        new VentilatorRequestHandler(LAMBDA_CLIENT, logger, WORKER_LAMBDAS, PARTITION_SIZE);
    handler.handleRequest();
    return null; // TODO What should this return? Status code?
  }
}
