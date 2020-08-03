package model.identity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedSet;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Immutable {@link SortedSet} of {@link UserId}s. "Destructive" operations (i.e. any modification
 * to the contents of the collection) throw a {@link UnsupportedOperationException} when called.
 */
public final class UserGroup implements WritableComparable<UserGroup>, SortedSet<UserId> {

  private static final Logger log = LoggerFactory.getLogger(UserGroup.class);

  private static final String NO_USERS_MESSAGE = "A UserGroup must contain at least one user";

  private static final String IMMUTABLE_MESSAGE = "Cannot modify an immutable collection";

  private SortedSet<UserId> users = ImmutableSortedSet.of();

  @JsonCreator
  private UserGroup(Collection<UserId> userIds) {
    Preconditions.checkNotNull(userIds);
    Preconditions.checkArgument(!userIds.isEmpty(), NO_USERS_MESSAGE);
    users = ImmutableSortedSet.copyOf(userIds);
  }

  private UserGroup() {
  }

  public static UserGroup fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating Users from DataInput");
    Preconditions.checkNotNull(dataInput);
    UserGroup allUsers = new UserGroup();
    allUsers.readFields(dataInput);
    return allUsers;
  }

  public static UserGroup fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating Users from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    SortedSet<UserId> userIds = new TreeSet<>();
    Iterator<JsonNode> userNodes = jsonNode.elements();
    while (userNodes.hasNext()) {
      userIds.add(UserId.fromJsonNode(userNodes.next()));
    }
    return new UserGroup(userIds);
  }

  public static UserGroup of(UserId... users) {
    return new UserGroup(new TreeSet<>(Arrays.asList(users)));
  }

  public static UserGroup of(Collection<UserId> users) {
    return new UserGroup(users);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    int nUsers = dataInput.readInt();
    SortedSet<UserId> usersInput = new TreeSet<>();
    for (int iUser = 0; iUser < nUsers; iUser++) {
      usersInput.add(UserId.fromDataInput(dataInput));
    }
    Preconditions.checkState(!usersInput.isEmpty(), NO_USERS_MESSAGE);
    users = ImmutableSortedSet.copyOf(usersInput);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeInt(users.size());
    for (UserId user : users) {
      user.write(dataOutput);
    }
  }

  @Override
  public int compareTo(UserGroup o) {
    Iterator<UserId> thisIter = users.iterator();
    Iterator<UserId> otherIter = o.iterator();
    int compare = 0;
    while (thisIter.hasNext() && otherIter.hasNext() && 0 == compare) {
      compare += thisIter.next().compareTo(otherIter.next());
    }
    return compare;
  }

  @Override
  public int size() {
    return users.size();
  }

  @Override
  public boolean isEmpty() {
    return users.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    return users.contains(o);
  }

  @Override
  public Iterator<UserId> iterator() {
    return users.iterator();
  }

  @Override
  public Object[] toArray() {
    return users.toArray();
  }

  @Override
  public <T> T[] toArray(T[] a) {
    return users.toArray(a);
  }

  @Override
  public boolean add(UserId e) {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public boolean remove(Object o) {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public boolean containsAll(Collection<?> c) {
    return users.containsAll(c);
  }

  @Override
  public boolean addAll(Collection<? extends UserId> c) {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public boolean retainAll(Collection<?> c) {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public boolean removeAll(Collection<?> c) {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException(IMMUTABLE_MESSAGE);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    UserGroup otherUsers = (UserGroup) o;
    return Objects.equals(users, otherUsers.getUsers());
  }

  @Override
  public int hashCode() {
    return Objects.hash(users);
  }

  @Override
  public String toString() {
    return MessageFormat.format("UserGroup'{'users={0}'}'", users);
  }

  public SortedSet<UserId> getUsers() {
    return ImmutableSortedSet.copyOf(users);
  }

  @Override
  public Comparator<? super UserId> comparator() {
    return users.comparator();
  }

  @Override
  public SortedSet<UserId> subSet(UserId fromElement, UserId toElement) {
    return users.subSet(fromElement, toElement);
  }

  @Override
  public SortedSet<UserId> headSet(UserId toElement) {
    return users.headSet(toElement);
  }

  @Override
  public SortedSet<UserId> tailSet(UserId fromElement) {
    return users.tailSet(fromElement);
  }

  @Override
  public UserId first() {
    return users.first();
  }

  @Override
  public UserId last() {
    return users.last();
  }
}
