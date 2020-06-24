package model;

import java.util.*;

public class ContactHistory
{
    private final Map<Date, Set<Contact>> history;

    public ContactHistory(Map<Date, Set<Contact>> history)
    {
        this.history = Collections.unmodifiableMap(history);
    }

    public boolean contactedBy(Contact contact)
    {
        return getContacts().contains(contact);
    }

    public Set<Date> getDates()
    {
        return getHistory().keySet();
    }

    public Set<Contact> getContacts()
    {
        Set<Contact> allContacts = new HashSet<>();
        getHistory().values().forEach(allContacts::addAll);
        return allContacts;
    }

    private Map<Date, Set<Contact>> getHistory()
    {
        return history;
    }
}
