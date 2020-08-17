package org.sharetrace.beliefpropagation.format.writable;

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
import org.sharetrace.model.identity.IdGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper type for {@link IdGroup} that is used in Hadoop.
 *
 * @see WritableComparable
 * @see IdGroup
 */
public final class FactorGraphVertexId implements WritableComparable<FactorGraphVertexId> {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorGraphVertexId.class);

  private IdGroup idGroup;

  private FactorGraphVertexId() {
  }

  private FactorGraphVertexId(IdGroup userGroup) {
    Preconditions.checkNotNull(userGroup);
    this.idGroup = IdGroup.copyOf(userGroup);
  }

  public static FactorGraphVertexId of(IdGroup userGroup) {
    return new FactorGraphVertexId(userGroup);
  }

  public static FactorGraphVertexId fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    FactorGraphVertexId writableComparable = new FactorGraphVertexId();
    writableComparable.readFields(dataInput);
    return writableComparable;
  }

  @Override
  public int compareTo(FactorGraphVertexId o) {
    Preconditions.checkNotNull(o);
    Iterator<String> thisIter = idGroup.getIds().iterator();
    Iterator<String> thatIter = o.getIdGroup().getIds().iterator();
    int nCompare = 0;
    while (thisIter.hasNext() && thatIter.hasNext()) {
      nCompare += thisIter.next().compareTo(thatIter.next());
    }
    return nCompare;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeInt(idGroup.getIds().size());
    for (String userId : idGroup.getIds()) {
      dataOutput.writeUTF(userId);
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    Collection<String> userIds = new TreeSet<>();
    int nUserIds = dataInput.readInt();
    for (int iUserId = 0; iUserId < nUserIds; iUserId++) {
      userIds.add(dataInput.readUTF());
    }
    idGroup = IdGroup.builder().setIds(userIds).build();
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'userGroup={1}'}'", getClass().getSimpleName(), idGroup);
  }

  public IdGroup getIdGroup() {
    return IdGroup.copyOf(idGroup);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    FactorGraphVertexId thatIdGroup = (FactorGraphVertexId) o;
    return Objects.equals(idGroup, thatIdGroup.getIdGroup());
  }

  @Override
  public int hashCode() {
    return Objects.hash(idGroup);
  }
}
