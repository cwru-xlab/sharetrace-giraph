package org.sharetrace.model.pda.response;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.sharetrace.model.util.ShareTraceUtil;

public final class ResponseUtil {

  private static final String INVALID_SUCCESSFUL_RESPONSE_MSG =
      "A successful response must only contain data of the response, and not an error code with a message/cause";

  private static final String INVALID_ERROR_RESPONSE_MSG =
      "An error response must only contain an error code and a message/cause, and not the data of the response";

  private static final String ERROR = "error";

  private static final String CAUSE = "cause";

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  static <T> void verifyInputArguments(Response<T> response) {
    Optional<List<T>> data = response.getData();
    Optional<String> error = response.getError();
    Optional<String> cause = response.getCause();
    boolean onlyError = error.isPresent() && cause.isPresent();

    if (data.isPresent()) {
      if (data.get().isEmpty()) {
        Preconditions.checkArgument(onlyError, INVALID_ERROR_RESPONSE_MSG);
      } else {
        boolean onlyData = !error.isPresent() && !cause.isPresent();
        Preconditions.checkArgument(onlyData, INVALID_SUCCESSFUL_RESPONSE_MSG);
      }
    } else {
      Preconditions.checkArgument(onlyError, INVALID_SUCCESSFUL_RESPONSE_MSG);
    }
  }

  public static ShortLivedTokenResponse mapToShortLivedTokenResponse(InputStream response)
      throws IOException {
    ShortLivedTokenResponse tokenResponse;
    try {
      tokenResponse = getSuccessfulTokenResponse(response);
    } catch (IOException | NullPointerException e) {
      tokenResponse = getFailedTokenResponse(response);
    }
    return tokenResponse;
  }

  private static ShortLivedTokenResponse getSuccessfulTokenResponse(InputStream response)
      throws IOException {
    Map<String, Object> tokenResponse = MAPPER.readValue(response,
        new TypeReference<Map<String, Object>>() {
        });
    String token = (String) tokenResponse.get("token");
    List<String> hats = (List<String>) tokenResponse.get("associatedHats");
    return ShortLivedTokenResponse.builder().shortLivedToken(token).data(hats).build();
  }

  private static ShortLivedTokenResponse getFailedTokenResponse(InputStream response)
      throws IOException {
    Map<String, String> failedResponse = getFailedResponse(response);
    return ShortLivedTokenResponse.builder()
        .error(failedResponse.get(ERROR))
        .cause(failedResponse.get(CAUSE))
        .build();
  }

  public static <T> PdaResponse<T> mapToPdaResponse(InputStream response) throws IOException {
    PdaResponse<T> pdaResponse;
    try {
      pdaResponse = getSuccessfulPdaResponse(response);
    } catch (IOException e) {
      pdaResponse = getFailedPdaResponse(response);
    }
    return pdaResponse;
  }

  private static <T> PdaResponse<T> getSuccessfulPdaResponse(InputStream response)
      throws IOException {
    List<Record<T>> records = MAPPER.readValue(response, new TypeReference<List<Record<T>>>() {
    });
    return PdaResponse.<T>builder().data(records).build();
  }

  private static <T> PdaResponse<T> getFailedPdaResponse(InputStream response) throws
      IOException {
    Map<String, String> failedResponse = getFailedResponse(response);
    return PdaResponse.<T>builder()
        .error(failedResponse.get(ERROR))
        .cause(failedResponse.get(CAUSE))
        .build();
  }

  private static Map<String, String> getFailedResponse(InputStream response) throws IOException {
    return  MAPPER.readValue(response, new TypeReference<Map<String, String>>() {
    });
  }
}

