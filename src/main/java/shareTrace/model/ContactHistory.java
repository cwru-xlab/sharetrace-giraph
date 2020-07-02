package model;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.time.Instant;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Value @Builder public class ContactHistory
{
    @NonNull Map<Instant, Set<Contact>> history;

    public boolean contactedBy(UserID<?> userID)
    {
        return getContacts().stream().anyMatch(c -> c.containsUser(userID));
    }

    public Set<Instant> getTimesOfContact()
    {
        return getHistory().keySet();
    }

    public Set<Contact> getContacts()
    {
        Set<Contact> allContacts = new HashSet<>();
        getHistory().values().forEach(allContacts::addAll);
        return allContacts;
    }
}
