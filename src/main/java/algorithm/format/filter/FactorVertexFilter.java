package algorithm.format.filter;

import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;
import model.contact.Contact;
import model.identity.UserGroup;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.hadoop.io.NullWritable;

@Log4j2
@NoArgsConstructor
public final class FactorVertexFilter implements
    VertexInputFilter<UserGroup, Contact, NullWritable> {

  @Override
  public boolean dropVertex(Vertex<UserGroup, Contact, NullWritable> vertex) {
    return vertex.getValue().getOccurrences().isEmpty();
  }
}
