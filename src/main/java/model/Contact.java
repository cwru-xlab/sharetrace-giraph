package model;

import java.time.Duration;
import java.util.Date;

public class Contact
{
    private final UserID<Long> firstUserId;
    private final UserID<Long> secondUserId;
    private final Date timeOfContact;
    private final Duration durationOfContact;
    private final SignalStrength signalStrength;

    public Contact(UserID<Long> firstUserId,
                   UserID<Long> secondUserId,
                   Date timeOfContact,
                   Duration durationOfContact,
                   SignalStrength signalStrength)
    {
        this.firstUserId = firstUserId.copy();
        this.secondUserId = secondUserId.copy();
        this.timeOfContact = timeOfContact;
        this.durationOfContact = durationOfContact;
        this.signalStrength = signalStrength.copy();
    }

    public UserID<Long> getFirstUserId()
    {
        return firstUserId;
    }

    public UserID<Long> getSecondUserId()
    {
        return secondUserId;
    }

    public Date getTimeOfContact()
    {
        return timeOfContact;
    }

    public Duration getDurationOfContact()
    {
        return durationOfContact;
    }

    public SignalStrength getSignalStrength()
    {
        return signalStrength;
    }
}
