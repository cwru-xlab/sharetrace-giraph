package server;

import model.AccessToken;

import java.io.Closeable;

/**
 * High-level API that initiates communication
 */
// TODO Consider using EventListener https://docs.oracle
//  .com/en/java/javase/14/docs/api/java.base/java/util/EventListener.html
public abstract class TokenMicroserver
        implements Runnable, Closeable, TokenAccessor<String>
{
    private final ShareTraceServer shareTraceServer;

    public TokenMicroserver(ShareTraceServer shareTraceServer)
    {
        this.shareTraceServer = shareTraceServer;
    }

    @Override
    public void updateToken(AccessToken<String> accessToken)
    {
        getShareTraceServer().updateToken(accessToken);
    }

    public ShareTraceServer getShareTraceServer()
    {
        return shareTraceServer;
    }
}
