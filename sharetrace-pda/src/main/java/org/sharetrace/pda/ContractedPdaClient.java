package org.sharetrace.pda;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.sharetrace.model.pda.request.ContractedPdaReadRequest;
import org.sharetrace.model.pda.request.ContractedPdaRequestBody;
import org.sharetrace.model.pda.request.ContractedPdaWriteRequest;
import org.sharetrace.model.pda.request.PdaRequestUrl;
import org.sharetrace.model.pda.request.ShortLivedTokenRequest;
import org.sharetrace.model.pda.response.PdaResponse;
import org.sharetrace.model.pda.response.ResponseUtil;
import org.sharetrace.model.pda.response.ShortLivedTokenResponse;
import org.sharetrace.model.util.ShareTraceUtil;

/**
 * Client that is able to send requests to contracted PDAs and retrieve a short-lived token.
 */
public class ContractedPdaClient {

  private static final ObjectMapper MAPPER = ShareTraceUtil.getMapper();

  private static final OkHttpClient CLIENT = new OkHttpClient();

  private static final String CONTRACTS_KEYRING = "contracts/keyring";

  private static final String AUTH_HEADER = "Authorization: Bearer %s";

  private static final String GET = "GET";

  private static final String POST = "POST";

  private static final String CONTENT_TYPE = "application/json";

  ShortLivedTokenResponse getShortLivedToken(ShortLivedTokenRequest request) throws IOException {
    HttpUrl url = new HttpUrl.Builder()
        .addPathSegments(request.getContractsServerUrl().getPath())
        .addPathSegments(CONTRACTS_KEYRING)
        .build();
    String token = request.getLongLivedToken();
    Request formattedRequest = new Request.Builder()
        .url(url)
        .method(GET, null)
        .addHeader(AUTH_HEADER, token).build();
    Response response = CLIENT.newCall(formattedRequest).execute();
    InputStream responseBody = Objects.requireNonNull(response.body()).byteStream();
    return ResponseUtil.mapToShortLivedTokenResponse(responseBody);
  }

  <T> PdaResponse<T> read(ContractedPdaReadRequest request) throws IOException {
    PdaRequestUrl url = request.getPdaRequestUrl();
    ContractedPdaRequestBody body = request.getReadRequestBody().getBaseRequestBody();
    InputStream response = request(url, body);
    return ResponseUtil.mapToPdaResponse(response);
  }

  <T> PdaResponse<T> write(ContractedPdaWriteRequest<T> request) throws IOException {
    PdaRequestUrl url = request.getPdaRequestUrl();
    ContractedPdaRequestBody body = request.getWriteRequestBody().getBaseRequestBody();
    InputStream response = request(url, body);
    return ResponseUtil.mapToPdaResponse(response);
  }

  InputStream request(PdaRequestUrl url, ContractedPdaRequestBody body) throws IOException {
    String textBody = MAPPER.writeValueAsString(body);
    MediaType mediaType = MediaType.parse(CONTENT_TYPE);
    RequestBody requestBody = RequestBody.create(textBody, mediaType);
    Request request = new Request.Builder()
        .url(url.toURL(body.getHatName()))
        .method(POST, requestBody)
        .build();
    Response response = CLIENT.newCall(request).execute();
    return Objects.requireNonNull(response.body()).byteStream();
  }
}
