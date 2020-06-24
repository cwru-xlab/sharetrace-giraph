package server;

import model.AccessToken;

// Uses the proxy design pattern
// TODO Make this a listener to AccessToken<T>
public interface TokenAccessor<T>
{
    public void updateToken(AccessToken<T> accessToken);
}
