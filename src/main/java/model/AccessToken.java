package main.java.model;

import lombok.NonNull;
import lombok.Value;

/**
 * A generic access token intended to be used for writing data to an authenticated entity.
 *
 * @param <T> Type of token.
 */
@Value(staticConstructor = "of") public class AccessToken<T>
{
    @NonNull T token;
}
