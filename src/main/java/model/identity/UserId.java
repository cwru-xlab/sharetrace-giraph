package model.identity;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

/**
 * An identifier for a user.
 */
@Log4j2
@Value(staticConstructor = "of")
public class UserId implements Identifiable<String>
{
    @NonNull
    @Getter(AccessLevel.NONE)
    String id;

    @Override
    public String getId()
    {
        return id;
    }

    @Override
    public int compareTo(@NonNull Identifiable<String> o)
    {
        return id.compareTo(o.getId());
    }
}
