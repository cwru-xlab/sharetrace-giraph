package model.contact;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import lombok.extern.log4j.Log4j2;
import model.identity.Identifiable;
import model.identity.UserId;
import org.apache.hadoop.io.Writable;

/**
 * An encounter between two users at one or more points in time.
 * <p>
 * Two contacts are considered equal if their contacts are equal, regardless of the field they are
 * assigned. That is, a {@code Contact} with {@link #firstUser} {@code u1} and {@link #secondUser}
 * {@code u2} is considered equal to a different {@code Contact} with equal {@code u1} and {@code
 * u2} except assigned to the opposite field.
 */
@Log4j2
public final class Contact implements Comparable<Contact>, Writable {

  private static final String NO_OCCURRENCES_MESSAGE = "A contact must contain at least one occurrence";

  private static final String IDENTICAL_USERS_MESSAGE = "A contact must contain two distinct users";

  private static final String FIRST_USER_LABEL = "firstUser";

  private static final String SECOND_USER_LABEL = "secondUser";

  private static final String OCCURRENCES_LABEL = "occurrences";

  private UserId firstUser;

  private UserId secondUser;

  private SortedSet<TemporalOccurrence> occurrences;

  @JsonCreator
  private Contact(UserId firstUser, UserId secondUser, SortedSet<TemporalOccurrence> occurrences) {
    Preconditions.checkNotNull(firstUser);
    Preconditions.checkNotNull(secondUser);
    Preconditions.checkNotNull(occurrences);
    Preconditions.checkArgument(!firstUser.equals(secondUser), IDENTICAL_USERS_MESSAGE);
    Preconditions.checkArgument(!occurrences.isEmpty(), NO_OCCURRENCES_MESSAGE);
    this.firstUser = firstUser;
    this.secondUser = secondUser;
    this.occurrences = ImmutableSortedSet.copyOf(occurrences);
  }

  private Contact() {
  }

  public static Contact fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating a Contact from DataInput");
    Preconditions.checkNotNull(dataInput);
    Contact contact = new Contact();
    contact.readFields(dataInput);
    return contact;
  }

  public static Contact fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating a Contact from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    Iterator<JsonNode> occurrencesIterator = jsonNode.get(OCCURRENCES_LABEL).elements();
    SortedSet<TemporalOccurrence> occurrences = new TreeSet<>();
    while (occurrencesIterator.hasNext()) {
      occurrences.add(TemporalOccurrence.fromJsonNode(occurrencesIterator.next()));
    }
    UserId firstUser = UserId.fromJsonNode(jsonNode.get(FIRST_USER_LABEL));
    UserId secondUser = UserId.fromJsonNode(jsonNode.get(SECOND_USER_LABEL));
    return new Contact(firstUser, secondUser, occurrences);
  }

  public static Contact of(Identifiable<String> firstUser,
      Identifiable<String> secondUser,
      SortedSet<TemporalOccurrence> occurrences) {
    return new Contact(UserId.of(firstUser.getId()), UserId.of(secondUser.getId()), occurrences);
  }

  public static Contact of(Identifiable<String> firstUser,
      Identifiable<String> secondUser,
      TemporalOccurrence... occurrences) {
    return of(firstUser, secondUser, new TreeSet<>(Arrays.asList(occurrences)));
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    setFirstUser(UserId.fromDataInput(dataInput));
    setSecondUser(UserId.fromDataInput(dataInput));
    int nOccurrences = dataInput.readInt();
    SortedSet<TemporalOccurrence> occurrences = new TreeSet<>();
    for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++) {
      occurrences.add(TemporalOccurrence.fromDataInput(dataInput));
    }
    setOccurrences(occurrences);
  }

  @Override
  public int compareTo(Contact o) {
    Preconditions.checkNotNull(o);
    int compare = firstUser.compareTo(o.getFirstUser());
    {
      if (0 == compare) {
        compare = secondUser.compareTo(o.getSecondUser());
      }
    }
    return compare;
  }

  public UserId getFirstUser() {
    return UserId.of(firstUser.getId());
  }

  private void setFirstUser(Identifiable<String> user) {
    Preconditions.checkNotNull(user);
    Preconditions.checkArgument(!user.equals(secondUser), IDENTICAL_USERS_MESSAGE);
    firstUser = UserId.of(user.getId());
  }

  public UserId getSecondUser() {
    return UserId.of(secondUser.getId());
  }

  private void setSecondUser(Identifiable<String> user) {
    Preconditions.checkNotNull(user);
    Preconditions.checkArgument(!user.equals(firstUser), IDENTICAL_USERS_MESSAGE);
    secondUser = UserId.of(user.getId());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    Contact contact = (Contact) o;
    boolean withSameOrder =
        firstUser.equals(contact.getFirstUser()) && secondUser.equals(contact.getSecondUser());
    boolean withDiffOrder =
        firstUser.equals(contact.getSecondUser()) && secondUser.equals(contact.getFirstUser());
    return withSameOrder || withDiffOrder;
  }

  @Override
  public int hashCode() {
    return Objects.hash(firstUser.getId(), secondUser.getId());
  }

  @Override
  public String toString() {
    return MessageFormat
        .format("Contact'{'firstUser={0}, secondUser={1}, temporalOccurrences={2}'}'",
            firstUser,
            secondUser,
            occurrences);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    firstUser.write(dataOutput);
    secondUser.write(dataOutput);
    dataOutput.writeInt(occurrences.size());
    for (TemporalOccurrence occurrence : occurrences) {
      occurrence.write(dataOutput);
    }
  }

  public SortedSet<TemporalOccurrence> getOccurrences() {
    return ImmutableSortedSet.copyOf(occurrences);
  }

  private void setOccurrences(SortedSet<TemporalOccurrence> occurrences) {
    Preconditions.checkNotNull(occurrences);
    Preconditions.checkState(!this.occurrences.isEmpty(), NO_OCCURRENCES_MESSAGE);
    this.occurrences = ImmutableSortedSet.copyOf(occurrences);
  }
}
