package sharetrace.pda.put;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class RiskScorePutRequestHandler implements RequestHandler<PDAPutRequest, Void> {

  @Override
  public Void handleRequest(PDAPutRequest input, Context context) {

    return null;
  }
}
