package sharetrace.algorithm.beliefpropagation.filter;

import java.time.Instant;
import java.util.SortedSet;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.format.vertex.VertexType;
import sharetrace.algorithm.beliefpropagation.format.writable.ContactWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.FactorGraphWritable;
import sharetrace.algorithm.beliefpropagation.format.writable.UserGroupWritableComparable;
import sharetrace.algorithm.beliefpropagation.param.BPContext;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.Occurrence;

/**
 * Factor vertex filter that prevents factor vertices from being added to the factor graph if all
 * occurrences of a {@link Contact} occurred prior to a set cutoff.
 */
public class ExpiredFactorVertexFilter implements VertexInputFilter<UserGroupWritableComparable,
    FactorGraphWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpiredFactorVertexFilter.class);

  private static final Instant CUTOFF = BPContext.getOccurrenceLookbackCutoff();

  @Override
  public boolean dropVertex(
      Vertex<UserGroupWritableComparable, FactorGraphWritable, NullWritable> vertex) {
    boolean shouldDrop;
    if (vertex.getValue().getType().equals(VertexType.VARIABLE)) {
      shouldDrop = true;
    } else {
      Contact factorData = ((ContactWritable) vertex.getValue().getWrapped()).getContact();
      SortedSet<Occurrence> occurrences = factorData.getOccurrences();
      shouldDrop = occurrences.last().getTime().isBefore(CUTOFF);
    }
    return shouldDrop;
  }
}
