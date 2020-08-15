package sharetrace.algorithm.beliefpropagation.combiner;

import org.apache.giraph.graph.VertexValueCombiner;
import sharetrace.model.contact.Contact;
import sharetrace.model.contact.ContactWritable;

public class ContactCombiner implements VertexValueCombiner<ContactWritable> {

  @Override
  public void combine(ContactWritable original, ContactWritable other) {
    Contact originalValue = original.getContact();
    Contact otherValue = other.getContact();
    Contact combined = Contact.builder()
        .setFirstUser(originalValue.getFirstUser())
        .setSecondUser(originalValue.getSecondUser())
        .addAllOccurrences(originalValue.getOccurrences())
        .addAllOccurrences(otherValue.getOccurrences())
        .build();
    original.setContact(combined);
  }
}
