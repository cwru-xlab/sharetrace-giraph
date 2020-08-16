package sharetrace.pda.put;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.net.URL;
import org.immutables.value.Value;
import sharetrace.model.score.RiskScore;

@JsonSerialize(as = PDAPutRequest.class)
@JsonDeserialize(as = PDAPutRequest.class)
@Value.Immutable
public abstract class AbstractPDAPutRequest {

  public abstract String getAccessToken();

  public abstract URL getUrl();

  public abstract RiskScore getRiskScore();
}
