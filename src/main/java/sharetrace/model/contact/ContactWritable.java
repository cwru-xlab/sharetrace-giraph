package sharetrace.model.contact;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.TreeSet;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.identity.UserId;

/**
 * Wrapper type for {@link Contact} that is used in Hadoop.
 *
 * @see Writable
 * @see Contact
 */
public final class ContactWritable implements Writable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ContactWritable.class);

  private AbstractContact contact;

  private ContactWritable() {
  }

  private ContactWritable(AbstractContact contact) {
    Preconditions.checkNotNull(contact);
    this.contact = Contact.copyOf(contact);
  }

  public static ContactWritable of(AbstractContact contact) {
    return new ContactWritable(contact);
  }

  public static ContactWritable fromDataInput(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    ContactWritable writable = new ContactWritable();
    writable.readFields(dataInput);
    return writable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeUTF(contact.getFirstUser().getId());
    dataOutput.writeUTF(contact.getSecondUser().getId());
    dataOutput.writeInt(contact.getOccurrences().size());
    for (AbstractOccurrence occurrence : contact.getOccurrences()) {
      dataOutput.writeLong(occurrence.getTime().getEpochSecond());
      dataOutput.writeLong(occurrence.getDuration().toSeconds());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    UserId firstUser = UserId.of(dataInput.readUTF());
    UserId secondUser = UserId.of(dataInput.readUTF());
    int nOccurrences = dataInput.readInt();
    Collection<Occurrence> occurrences = new TreeSet<>();
    for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++) {
      Instant time = Instant.ofEpochSecond(dataInput.readLong());
      Duration duration = Duration.ofSeconds(dataInput.readLong());
      occurrences.add(Occurrence.of(time, duration));
    }
    contact = Contact.builder()
        .setFirstUser(firstUser)
        .setSecondUser(secondUser)
        .setOccurrences(occurrences)
        .build();
  }

  public Contact getContact() {
    return Contact.copyOf(contact);
  }

  @Override
  public String toString() {
    return MessageFormat.format("{0}'{'contact={1}'}'", getClass().getSimpleName(), contact);
  }
}
