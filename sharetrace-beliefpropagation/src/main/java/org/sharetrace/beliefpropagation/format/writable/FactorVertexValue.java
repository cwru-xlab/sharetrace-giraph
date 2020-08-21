package org.sharetrace.beliefpropagation.format.writable;

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
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrapper type for {@link Contact} that is used in Hadoop.
 *
 * @see Writable
 * @see Contact
 */
public final class FactorVertexValue implements Writable {

  private static final Logger LOGGER = LoggerFactory.getLogger(FactorVertexValue.class);

  private Contact contact;

  private FactorVertexValue() {
  }

  private FactorVertexValue(Contact contact) {
    Preconditions.checkNotNull(contact, "Factor vertex value must not be null");
    this.contact = Contact.copyOf(contact);
  }

  public static FactorVertexValue of(Contact contact) {
    return new FactorVertexValue(contact);
  }

  public static FactorVertexValue fromDataInput(DataInput dataInput) throws IOException {
    FactorVertexValue writable = new FactorVertexValue();
    writable.readFields(dataInput);
    return writable;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput, "DataOutput must not be null");
    dataOutput.writeUTF(contact.getFirstUser());
    dataOutput.writeUTF(contact.getSecondUser());
    dataOutput.writeInt(contact.getOccurrences().size());
    for (Occurrence occurrence : contact.getOccurrences()) {
      dataOutput.writeLong(occurrence.getTime().toEpochMilli());
      dataOutput.writeLong(occurrence.getDuration().toMillis());
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput, "DataInput must not be null to read fields");
    String firstUser = dataInput.readUTF();
    String secondUser = dataInput.readUTF();
    int nOccurrences = dataInput.readInt();
    Collection<Occurrence> occurrences = new TreeSet<>();
    for (int iOccurrence = 0; iOccurrence < nOccurrences; iOccurrence++) {
      Instant time = Instant.ofEpochMilli(dataInput.readLong());
      Duration duration = Duration.ofMillis(dataInput.readLong());
      occurrences.add(Occurrence.builder().time(time).duration(duration).build());
    }
    contact = Contact.builder()
        .firstUser(firstUser)
        .secondUser(secondUser)
        .occurrences(occurrences)
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
