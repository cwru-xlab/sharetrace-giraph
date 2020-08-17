package org.sharetrace.model.vertex;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.immutables.value.Value;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.identity.IdGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Value.Immutable
@JsonSerialize(as = FactorVertex.class)
@JsonDeserialize(as = FactorVertex.class)
public abstract class AbstractFactorVertex implements FactorGraphVertex<IdGroup, Contact> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFactorVertex.class);

  @Value.Default
  @Override
  public VertexType getType(){
    return VertexType.FACTOR;
  }

  @Override
  public abstract IdGroup getVertexId();

  @Override
  public abstract Contact getVertexValue();
}
