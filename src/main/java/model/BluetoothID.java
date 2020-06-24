package model;

public class BluetoothID extends UserID<Long>
{
    private BluetoothID(Long id)
    {
        super(id);
    }

    @Override
    public UserID<Long> create(Long id)
    {
        return new BluetoothID(id);
    }
}
