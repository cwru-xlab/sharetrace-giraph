package model.identity;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.text.MessageFormat;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;

/**
 * A generic access token intended to be used for writing data to an authenticated entity.
 *
 * @see Identifiable
 */
@Log4j2
public final class AccessToken implements Identifiable<String> {

  private final String token;

  private AccessToken(String s) {
    Preconditions.checkArgument(Strings.isNullOrEmpty(s));
    token = s;
  }

  public static AccessToken of(String token) {
    return new AccessToken(token);
  }

  @Override
  public String getId() {
    return token;
  }

  @Override
  public int compareTo(Identifiable<String> o) {
    Preconditions.checkNotNull(o);
    return token.compareTo(o.getId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    Identifiable<String> accessToken = (Identifiable<String>) o;
    return Objects.equals(token, accessToken.getId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(token);
  }

  @Override
  public String toString() {
    return MessageFormat.format("AccessToken'{'token=''{0}'''}'", token);
  }
}
