package model.identity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Objects;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.io.Writable;
import org.python.google.common.base.Preconditions;

/**
 * An identifier for a user.
 *
 * @see Identifiable
 * @see Writable
 */
@Log4j2
public final class UserId implements Identifiable<String>, Writable {

  private static final String ID_LABEL = "id";

  private String id;

  @JsonCreator
  private UserId(String s) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(s));
    id = s;
  }

  private UserId() {
  }

  public static UserId fromDataInput(DataInput dataInput) throws IOException {
    log.debug("Creating UserId from DataInput");
    Preconditions.checkNotNull(dataInput);
    UserId user = new UserId();
    user.readFields(dataInput);
    return user;
  }

  public static UserId fromJsonNode(JsonNode jsonNode) {
    log.debug("Creating UserId from JsonNode");
    Preconditions.checkNotNull(jsonNode);
    return new UserId(jsonNode.get(ID_LABEL).asText());
  }

  public static UserId of(String id) {
    return new UserId(id);
  }

  public static String getIdLabel() {
    return ID_LABEL;
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    Preconditions.checkNotNull(dataInput);
    id = dataInput.readUTF();
  }

  @Override
  public int compareTo(Identifiable<String> o) {
    Preconditions.checkNotNull(o);
    return id.compareTo(o.getId());
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    Preconditions.checkNotNull(dataOutput);
    dataOutput.writeUTF(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    Identifiable<String> userId = (Identifiable<String>) o;
    return Objects.equals(id, userId.getId());
  }

  @JsonProperty("id")
  @Override
  public String getId() {
    return id;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }

  @Override
  public String toString() {
    return MessageFormat.format("UserId'{'id=''{0}'''}'", id);
  }
}
