package sharetrace.algorithm.beliefpropagation.filter;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.SortedSet;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sharetrace.algorithm.beliefpropagation.computation.MasterComputer;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.ContactWritable;
import sharetrace.model.contact.Occurrence;
import sharetrace.model.identity.UserGroupWritableComparable;

/**
 * Factor vertex filter that prevents factor vertices from being added to the factor graph if all
 * occurrences of a {@link Contact} occurred prior to a set cutoff.
 */
public class ExpiredFactorVertexFilter implements VertexInputFilter<UserGroupWritableComparable,
    ContactWritable, NullWritable> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpiredFactorVertexFilter.class);

  private static final long CUTOFF_VALUE = 14L;

  private static final ChronoUnit CUTOFF_TIME_UNIT = ChronoUnit.DAYS;

  private static final Instant CUTOFF = MasterComputer.getInitializedAt()
      .minus(CUTOFF_VALUE, CUTOFF_TIME_UNIT);

  @Override
  public boolean dropVertex(
      Vertex<UserGroupWritableComparable, ContactWritable, NullWritable> vertex) {
    SortedSet<Occurrence> occurrences = vertex.getValue().getContact().getOccurrences();
    return occurrences.last().getTime().isBefore(CUTOFF);
  }

  public static Instant getCutoff() {
    return CUTOFF;
  }
}
