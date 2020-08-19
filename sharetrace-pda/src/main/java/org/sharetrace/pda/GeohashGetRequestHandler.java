package org.sharetrace.pda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.sharetrace.model.location.LocationHistory;
import org.sharetrace.model.location.TemporalLocation;
import org.sharetrace.model.pda.GeohashPayload;
import org.sharetrace.model.pda.PDAGetRequest;
import org.sharetrace.model.util.ShareTraceUtil;

public class GeohashGetRequestHandler implements RequestHandler<PDAGetRequest, String> {

  private static final String ENVIRONMENT_VARIABLES = "ENVIRONMENT VARIABLES: ";

  private static final String CONTEXT = "CONTEXT: ";

  private static final String EVENT = "EVENT: ";

  private static final String EVENT_TYPE = "EVENT_TYPE: ";

  private static final StringBuilder STRING_BUILDER = new StringBuilder();

  private static final OkHttpClient CLIENT = new OkHttpClient();

  private static final String ORDERING = "ordering";

  private static final String ASCENDING = "ascending";

  private static final String ORDER_BY = "orderBy";

  private static final String TIMESTAMP = "timestamp";

  private static final String SKIP = "skip";

  private static final String GET = "GET";

  private static final String CONTENT_TYPE = "Content-Type";

  private static final String APPLICATION_JSON = "application/json";

  private static final String X_AUTH_TOKEN = "x-auth-token";

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  @Override
  public String handleRequest(PDAGetRequest input, Context context) {
    Preconditions.checkNotNull(input, "Input type must not be null");
    Preconditions.checkNotNull(context, "Context must not be null");
    log(input, context);
    LambdaLogger logger = context.getLogger();
    // TODO Get the number of entries to skip from S3
    int nRecordsToSkip = 1;
    HttpUrl url = HttpUrl.parse(input.getUrl().getPath()).newBuilder()
        .addQueryParameter(ORDERING, ASCENDING)
        .addQueryParameter(ORDER_BY, TIMESTAMP)
        .addQueryParameter(SKIP, String.valueOf(nRecordsToSkip))
        .build();
    Request request = new Request.Builder()
        .url(url)
        .method(GET, null)
        .addHeader(CONTENT_TYPE, APPLICATION_JSON)
        .addHeader(X_AUTH_TOKEN, input.getAccessToken())
        .build();
    try {
      Response response = CLIENT.newCall(request).execute();
      InputStream bodyStream = response.body().byteStream();
      GeohashPayload payload = MAPPER.readValue(bodyStream, GeohashPayload.class);
      Set<TemporalLocation> locations = payload.getLocations()
          .stream()
          .map(hash -> TemporalLocation.builder()
              .setLocation(hash.getLocation())
              .setTime(hash.getTime())
              .build())
          .collect(Collectors.toSet());
      // TODO This will be retrieved from the KeyRing response
      String userId = null;
      LocationHistory history = LocationHistory.builder()
          .setId(userId)
          .setHistory(locations)
          .build();
      // TODO Save history to S3 bucket
    } catch (IOException e) {
      logger.log(e.getMessage());
    }
    return null;
  }

  private void log(PDAGetRequest input, Context context) {
    LambdaLogger logger = context.getLogger();
    try {
      String environmentVariablesLog = STRING_BUILDER
          .append(ENVIRONMENT_VARIABLES)
          .append(MAPPER.writeValueAsString(System.getenv()))
          .toString();
      logger.log(environmentVariablesLog);
      resetStringBuilder();

      String contextLog = STRING_BUILDER
          .append(CONTEXT)
          .append(MAPPER.writeValueAsString(context))
          .toString();
      logger.log(contextLog);
      resetStringBuilder();

      String eventLog = STRING_BUILDER
          .append(EVENT)
          .append(MAPPER.writeValueAsString(input))
          .toString();
      logger.log(eventLog);
      resetStringBuilder();

      String eventTypeLog = STRING_BUILDER
          .append(EVENT_TYPE)
          .append(input.getClass().getSimpleName())
          .toString();
      logger.log(eventTypeLog);
      resetStringBuilder();
    } catch (JsonProcessingException e) {
      logger.log(e.getMessage());
    }
  }

  private void resetStringBuilder() {
    STRING_BUILDER.delete(0, STRING_BUILDER.length());
  }
}
