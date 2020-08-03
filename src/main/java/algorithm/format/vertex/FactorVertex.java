package algorithm.format.vertex;

import algorithm.format.vertex.serialization.FactorVertexDeserializer;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import java.text.MessageFormat;
import java.util.Objects;
import java.util.SortedSet;
import model.contact.Contact;
import model.contact.TemporalOccurrence;
import model.identity.Identifiable;
import model.identity.UserGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@JsonDeserialize(using = FactorVertexDeserializer.class)
public final class FactorVertex implements Vertex<UserGroup, Contact> {

  private static final Logger log = LoggerFactory.getLogger(FactorVertex.class);

  private final UserGroup vertexId;

  private final Contact vertexValue;

  @JsonCreator
  private FactorVertex(UserGroup vertexId, Contact vertexValue) {
    Preconditions.checkNotNull(vertexId);
    Preconditions.checkNotNull(vertexValue);
    this.vertexId = vertexId;
    this.vertexValue = vertexValue;
  }

  public static FactorVertex of(UserGroup vertexId, Contact vertexValue) {
    return new FactorVertex(vertexId, vertexValue);
  }

  @Override
  public UserGroup getVertexId() {
    return UserGroup.of(vertexId);
  }

  @Override
  public Contact getVertexValue() {
    Identifiable<String> firstUser = vertexValue.getFirstUser();
    Identifiable<String> secondUser = vertexValue.getSecondUser();
    SortedSet<TemporalOccurrence> occurrences = vertexValue.getOccurrences();
    return Contact.of(firstUser, secondUser, occurrences);
  }

  @Override
  public String toString() {
    String pattern = "FactorVertex'{'vertexId={0}, vertexValue={1}'}'";
    return MessageFormat.format(pattern, vertexId, vertexValue);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (null == o || getClass() != o.getClass()) {
      return false;
    }
    Vertex<UserGroup, Contact> otherFactorVertex = (Vertex<UserGroup, Contact>) o;
    boolean equalId = Objects.equals(getVertexId(), otherFactorVertex.getVertexId());
    boolean equalValue = Objects.equals(getVertexValue(), otherFactorVertex.getVertexValue());
    return equalId && equalValue;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getVertexId(), getVertexValue());
  }
}
