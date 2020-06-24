package model;

// TODO Make this listenable
public class AccessToken<T>
{
    private final T token;

    private AccessToken(T token)
    {
        this.token = token;
    }

    public AccessToken<T> create(T token)
    {
        return new AccessToken<>(token);
    }

    public AccessToken<T> copy()
    {
        return create(getToken());
    }

    public T getToken()
    {
        return token;
    }
}
