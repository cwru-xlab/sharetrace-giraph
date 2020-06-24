package server;

import model.AccessToken;

import java.io.Closeable;
import java.net.URL;

public abstract class ShareTraceServer
        implements Runnable, Closeable, TokenAccessor<String>
{
    public abstract void authenticatePDA(AccessToken<String> accessToken,
                                         URL PDA);

    public abstract void putToPDA(AccessToken<String> accessToken, URL PDA);

    public abstract void getFromPDA(AccessToken<String> accessToken, URL PDA);

    public abstract void computeRiskScores();
}
