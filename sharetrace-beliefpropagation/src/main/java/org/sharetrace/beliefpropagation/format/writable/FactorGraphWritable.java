package org.sharetrace.beliefpropagation.format.writable;

import com.google.common.base.Preconditions;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.sharetrace.model.vertex.VertexType;

public class FactorGraphWritable implements Writable {

  private VertexType type;

  private Writable wrapped;

  public static FactorGraphWritable fromDataInput(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);
    FactorGraphWritable writable = new FactorGraphWritable();
    writable.readFields(in);
    return writable;
  }

  public static FactorGraphWritable ofVariableVertex(Writable wrapped) {
    return new FactorGraphWritable(VertexType.VARIABLE, wrapped);
  }

  public static FactorGraphWritable ofFactorVertex(Writable wrapped) {
    return new FactorGraphWritable(VertexType.FACTOR, wrapped);
  }

  private FactorGraphWritable(VertexType type, Writable wrapped) {
    Preconditions.checkNotNull(type);
    Preconditions.checkNotNull(wrapped);
    this.type = type;
    this.wrapped = wrapped;
  }

  private FactorGraphWritable() {
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Preconditions.checkNotNull(out);
    out.writeUTF(type.toString());
    wrapped.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    Preconditions.checkNotNull(in);
    type = VertexType.valueOf(in.readUTF());
    wrapped.readFields(in);
  }

  public VertexType getType() {
    return type;
  }

  public Writable getWrapped() {
    return wrapped;
  }

  public void setWrapped(Writable wrapped) {
    this.wrapped = wrapped;
  }
}
