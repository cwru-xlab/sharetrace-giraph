package sharetrace.pda.get;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.net.URL;
import org.immutables.value.Value;

@JsonSerialize(as = PDAGetRequest.class)
@JsonDeserialize(as = PDAGetRequest.class)
@Value.Immutable
public abstract class AbstractPDAGetRequest {

  public abstract String getAccessToken();

  public abstract URL getUrl();

  public abstract String getUserId();

  @Value.Check
  protected final void verifyInputArguments() {
    Preconditions.checkArgument(Strings.isNullOrEmpty(getAccessToken()));
    Preconditions.checkNotNull(getUrl());
    Preconditions.checkArgument(Strings.isNullOrEmpty(getUrl().getPath()));
    Preconditions.checkArgument(Strings.isNullOrEmpty(getUserId()));
  }

}
