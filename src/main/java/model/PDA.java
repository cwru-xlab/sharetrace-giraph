package model;

import java.util.Collections;
import java.util.Set;

public abstract class PDA
{
    private final AccessToken<String> accessToken;
    private final UserID<Long> userID;
    private final Set<Symptom> symptoms;
    private final Boolean diagnosed;
    private final ContactHistory contactHistory;

    public PDA(AccessToken<String> accessToken,
               BluetoothID userID,
               Set<Symptom> symptoms,
               Boolean diagnosed,
               ContactHistory contactHistory)
    {
        this.accessToken = accessToken.copy();
        this.userID = userID.copy();
        this.symptoms = Collections.unmodifiableSet(symptoms);
        this.diagnosed = diagnosed;
        this.contactHistory = contactHistory;
    }

    public abstract UserID<Long> generateUserID();

    public AccessToken<String> getAccessToken()
    {
        return accessToken;
    }

    public UserID<Long> getUserID()
    {
        return userID;
    }

    public Set<Symptom> getSymptoms()
    {
        return symptoms;
    }

    public Boolean getDiagnosed()
    {
        return diagnosed;
    }

    public ContactHistory getContactHistory()
    {
        return contactHistory;
    }
}
