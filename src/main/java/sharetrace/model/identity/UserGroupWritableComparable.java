package sharetrace.model.identity;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Iterator;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.hadoop.io.WritableComparable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper type for {@link UserGroup} that is used in Hadoop.
 *
 * @see WritableComparable
 * @see UserGroup
 */
public final class UserGroupWritableComparable implements
    WritableComparable<UserGroupWritableComparable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(UserGroupWritableComparable.class);

  private AbstractUserGroup userGroup;

  private UserGroupWritableComparable() {
  }

  private UserGroupWritableComparable(AbstractUserGroup userGroup) {
    Preconditions.checkNotNull(userGroup);
    this.userGroup = UserGroup.copyOf(userGroup);
  }

  public static UserGroupWritableComparable of(AbstractUserGroup userGroup) {
    return new UserGroupWritableComparable(userGroup);
  }

  public static UserGroupWritableComparable fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    UserGroupWritableComparable writableComparable = new UserGroupWritableComparable();
    writableComparable.readFields(dataInput);
    return writableComparable;
  }

  @Override
  public int compareTo(UserGroupWritableComparable o) {
    Preconditions.checkNotNull(o);
    Iterator<UserId> thisIter = userGroup.getUsers().iterator();
    Iterator<UserId> thatIter = o.getUserGroup().getUsers().iterator();
    int nCompare = 0;
    while (thisIter.hasNext() && thatIter.hasNext()) {
      nCompare += thisIter.next().compareTo(thatIter.next());
    }
    return nCompare;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeInt(userGroup.getUsers().size());
    for (Identifiable<String> userId : userGroup.getUsers()) {
      dataOutput.writeUTF(userId.getId());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    Collection<UserId> userIds = new TreeSet<>();
    int nUserIds = dataInput.readInt();
    for (int iUserId = 0; iUserId < nUserIds; iUserId++) {
      userIds.add(UserId.of(dataInput.readUTF()));
    }
    userGroup = UserGroup.of(userIds);
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'userGroup={1}'}'", getClass().getSimpleName(), userGroup);
  }

  public UserGroup getUserGroup() {
    return UserGroup.copyOf(userGroup);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    UserGroupWritableComparable thatUserGroup = (UserGroupWritableComparable) o;
    return Objects.equals(userGroup, thatUserGroup.getUserGroup());
  }

  @Override
  public int hashCode() {
    return Objects.hash(userGroup);
  }
}
