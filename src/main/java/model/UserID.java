package model;

public abstract class UserID<T>
{
    private final T id;

    public UserID(T id)
    {
        this.id = id;
    }

    public abstract UserID<T> create(T id);

    public UserID<T> copy()
    {
        return create(getId());
    }

    public T getId()
    {
        return this.id;
    }
}
