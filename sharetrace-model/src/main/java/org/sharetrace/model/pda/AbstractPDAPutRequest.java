package org.sharetrace.model.pda;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URL;
import org.immutables.value.Value;
import org.sharetrace.model.score.RiskScore;

@Value.Immutable
@JsonSerialize(as = PDAPutRequest.class)
@JsonDeserialize(as = PDAPutRequest.class)
public abstract class AbstractPDAPutRequest {

  public abstract String getAccessToken();

  public abstract URL getUrl();

  public abstract RiskScore getRiskScore();
}
