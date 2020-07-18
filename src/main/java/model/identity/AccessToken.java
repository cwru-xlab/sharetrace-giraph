package model.identity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

/**
 * A generic access token intended to be used for writing data to an authenticated entity.
 */
@Log4j2
@Value(staticConstructor = "of")
public class AccessToken implements Identifiable<String>
{
    @NonNull
    @Getter(AccessLevel.NONE)
    String token;

    @Override
    public String getId()
    {
        return token;
    }

    @Override
    public int compareTo(@NonNull Identifiable<String> o)
    {
        return token.compareTo(o.getId());
    }
}
