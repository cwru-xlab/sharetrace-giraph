package org.sharetrace.beliefpropagation.filter;

import com.google.common.base.Preconditions;
import java.time.Instant;
import java.util.SortedSet;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.filters.VertexInputFilter;
import org.apache.hadoop.io.NullWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphVertexId;
import org.sharetrace.beliefpropagation.format.writable.FactorGraphWritable;
import org.sharetrace.beliefpropagation.format.writable.FactorVertexValue;
import org.sharetrace.beliefpropagation.param.BPContext;
import org.sharetrace.model.contact.Contact;
import org.sharetrace.model.contact.Occurrence;
import org.sharetrace.model.vertex.VertexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factor vertex filter that prevents factor vertices from being added to the factor graph if all
 * occurrences of a {@link Contact} occurred prior to a set cutoff.
 */
public class ExpiredFactorVertexFilter
    implements VertexInputFilter<FactorGraphVertexId, FactorGraphWritable, NullWritable> {

  // Logging messages
  private static final String VARIABLE_DROP_MSG = "Variable vertex will not be dropped";
  private static final String FACTOR_DROP_MSG = "Factor vertex with no occurrences will be dropped";
  private static final String NO_OCCURRENCES_DROP_MSG =
      "Factor vertex with no occurrences before cutoff will be dropped";
  private static final String OCCURRENCES_NO_DROP_MSG =
      "Factor vertex with at least 1 occurrence after cutoff not be dropped";

  private static final Logger LOGGER = LoggerFactory.getLogger(ExpiredFactorVertexFilter.class);

  private static final Instant CUTOFF = BPContext.getOccurrenceLookbackCutoff();

  @Override
  public boolean dropVertex(
      Vertex<FactorGraphVertexId, FactorGraphWritable, NullWritable> vertex) {
    Preconditions.checkNotNull(vertex);
    boolean shouldDrop;
    if (vertex.getValue().getType().equals(VertexType.VARIABLE)) {
      LOGGER.debug(VARIABLE_DROP_MSG);
      shouldDrop = false;
    } else {
      Contact factorData = ((FactorVertexValue) vertex.getValue().getWrapped()).getValue();
      SortedSet<Occurrence> occurrences = factorData.getOccurrences();
      if (occurrences.isEmpty()) {
        LOGGER.debug(FACTOR_DROP_MSG);
        shouldDrop = true;
      } else {
        shouldDrop = occurrences.last().getTime().isBefore(CUTOFF);
        if (shouldDrop) {
          LOGGER.debug(NO_OCCURRENCES_DROP_MSG);
        } else {
          LOGGER.debug(OCCURRENCES_NO_DROP_MSG);
        }
      }
    }
    return shouldDrop;
  }
}
