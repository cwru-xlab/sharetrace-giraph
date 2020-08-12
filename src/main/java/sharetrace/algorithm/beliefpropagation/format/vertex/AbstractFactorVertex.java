package sharetrace.algorithm.beliefpropagation.format.vertex;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.model.contact.Contact;
import sharetrace.model.identity.UserGroup;

@Value.Immutable
@JsonSerialize(as = FactorVertex.class)
@JsonDeserialize(as = FactorVertex.class)
public abstract class AbstractFactorVertex implements Vertex<UserGroup, Contact> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFactorVertex.class);

  @Override
  public abstract UserGroup getVertexId();

  @Override
  public abstract Contact getVertexValue();
}
