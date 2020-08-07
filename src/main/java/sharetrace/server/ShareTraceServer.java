package sharetrace.server;

import java.io.Closeable;
import java.net.URL;
import sharetrace.model.identity.AccessToken;

public abstract class ShareTraceServer implements Runnable, Closeable {

  public abstract void authenticatePDA(AccessToken accessToken, URL PDA);

  public abstract void putToPDA(AccessToken accessToken, URL PDA);

  public abstract void getFromPDA(AccessToken accessToken, URL PDA);
}
