package sharetrace.pda.get;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.squareup.okhttp.HttpUrl;
import com.squareup.okhttp.OkHttpClient;
import com.squareup.okhttp.Request;
import com.squareup.okhttp.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;
import java.util.stream.Collectors;
import sharetrace.model.identity.UserId;
import sharetrace.model.location.LocationHistory;
import sharetrace.model.location.TemporalLocation;

public class GeohashGetRequestHandler implements RequestHandler<PDAGetRequest, Void> {

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

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // TODO Write to a single file
  @Override
  public Void handleRequest(PDAGetRequest input, Context context) {
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
      GeohashPayload payload = OBJECT_MAPPER.readValue(bodyStream, GeohashPayload.class);
      Set<TemporalLocation> locations = payload.getGeohashes()
          .stream()
          .map(hash -> TemporalLocation.of(hash.getHash(), hash.getTimestamp()))
          .collect(Collectors.toSet());
      UserId userId = UserId.of(input.getUserId());
      LocationHistory history = LocationHistory.of(userId, locations);
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
          .append(OBJECT_MAPPER.writeValueAsString(System.getenv()))
          .toString();
      logger.log(environmentVariablesLog);
      resetStringBuilder();

      String contextLog = STRING_BUILDER
          .append(CONTEXT)
          .append(OBJECT_MAPPER.writeValueAsString(context))
          .toString();
      logger.log(contextLog);
      resetStringBuilder();

      String eventLog = STRING_BUILDER
          .append(EVENT)
          .append(OBJECT_MAPPER.writeValueAsString(input))
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
