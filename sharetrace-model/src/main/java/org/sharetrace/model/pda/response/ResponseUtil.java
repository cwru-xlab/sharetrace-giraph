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

  private static final String MESSAGE = "message";

  private static final String CAUSE = "cause";

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  static <T> void verifyInputArguments(Response<T> response) {
    Optional<List<T>> dataOptional = response.getData();
    Optional<String> errorOptional = response.getError();
    Optional<String> messageOptional = response.getMessage();
    Optional<String> causeOptional = response.getCause();
    boolean dataResponse = dataOptional.isPresent();
    boolean errorResponse =
        errorOptional.isPresent() && (messageOptional.isPresent() || causeOptional.isPresent());
    boolean dataOnly = dataResponse && !errorResponse;
    boolean errorOnly = !dataResponse && errorResponse;

    if (dataOptional.isPresent()) {
      Preconditions.checkArgument(dataOnly, INVALID_SUCCESSFUL_RESPONSE_MSG);
    } else {
      Preconditions.checkArgument(errorOnly, INVALID_ERROR_RESPONSE_MSG);
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
    Map<String, String> failedResponse =
        MAPPER.readValue(response, new TypeReference<Map<String, String>>() {
        });
    ShortLivedTokenResponse tokenResponse;
    try {
      tokenResponse = ShortLivedTokenResponse.builder()
          .error(failedResponse.get(ERROR))
          .message(failedResponse.get(MESSAGE))
          .build();
    } catch (NullPointerException e) {
      tokenResponse = ShortLivedTokenResponse.builder()
          .error(failedResponse.get(ERROR))
          .cause(failedResponse.get(CAUSE))
          .build();
    }
    return tokenResponse;
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

  private static <T> PdaResponse<T> getFailedPdaResponse(InputStream response) throws IOException {
    Map<String, String> failedResponse =
        MAPPER.readValue(response, new TypeReference<Map<String, String>>() {
        });
    PdaResponse<T> pdaResponse;
    try {
      pdaResponse = PdaResponse.<T>builder()
          .error(failedResponse.get(ERROR))
          .message(failedResponse.get(MESSAGE))
          .build();
    } catch (NullPointerException e) {
      pdaResponse = PdaResponse.<T>builder()
          .error(failedResponse.get(ERROR))
          .cause(failedResponse.get(CAUSE))
          .build();
    }
    return pdaResponse;
  }
}

