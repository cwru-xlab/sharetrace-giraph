package org.sharetrace.pda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.sharetrace.model.pda.PDAPutRequest;

public class RiskScorePutRequestHandler implements RequestHandler<PDAPutRequest, Void> {

  @Override
  public Void handleRequest(PDAPutRequest input, Context context) {

    return null;
  }
}
