package org.sharetrace.model.pda.request;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.URL;
import okhttp3.HttpUrl;
import org.immutables.value.Value;

/**
 * Url of a request to a PDA.
 */
@Value.Immutable
@JsonSerialize(as = PdaRequestUrl.class)
@JsonDeserialize(as = PdaRequestUrl.class)
public abstract class AbstractPdaRequestUrl {

  private static final String INVALID_NAMESPACE_MSG = "Namespace must not be empty String or null";

  private static final String INVALID_ENDPOINT_MSG = "Endpoint must not be empty String or null";

  private static final String INVALID_HAT_NAME_MSG = "Hat name must not be empty String or null";

  private static final String API_VERSION = "v2.6";

  private static final String SCHEME = "https";

  private static final String HAT_DOMAIN_DELIMITER = ".";

  private static final String SANDBOX_DOMAIN = "hubat.net";

  private static final String PROD_DOMAIN = "hubofallthings.net";

  private static final String API = "api";

  private static final String DATA = "data";

  private static final String CONTRACT_DATA = "contract-data";

  public abstract String getNamespace();

  public abstract String getEndpoint();

  @Value.Default
  public boolean isSandbox() {
    return true;
  }

  @Value.Default
  public boolean isContracted() {
    return false;
  }

  public abstract Operation getOperation();

  @Value.Derived
  public URL toURL(String hatName) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(hatName), INVALID_HAT_NAME_MSG);
    HttpUrl.Builder builder = new HttpUrl.Builder().scheme(SCHEME);
    if (isSandbox()) {
      builder.host(formatDomainWithHat(hatName, SANDBOX_DOMAIN));
    } else {
      builder.host(formatDomainWithHat(hatName, PROD_DOMAIN));
    }
    builder.addPathSegment(API).addPathSegment(API_VERSION);
    if (isContracted()) {
      builder.addPathSegment(CONTRACT_DATA);
    } else {
      builder.addPathSegment(DATA);
    }
    return builder.addPathSegment(getOperation().toString().toLowerCase())
        .addPathSegment(getNamespace())
        .addPathSegment(getEndpoint())
        .build()
        .url();
  }

  private String formatDomainWithHat(String hatName, String domain) {
    return hatName + HAT_DOMAIN_DELIMITER + domain;
  }

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getNamespace()), INVALID_NAMESPACE_MSG);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(getEndpoint()), INVALID_ENDPOINT_MSG);
  }

  public enum Operation {
    CREATE, READ;
  }
}
