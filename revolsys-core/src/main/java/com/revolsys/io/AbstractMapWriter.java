package com.revolsys.io;

import java.util.Map;

import com.revolsys.io.map.MapWriter;

public abstract class AbstractMapWriter extends AbstractWriter<Map<String, ? extends Object>>
  implements MapWriter {
  private boolean writeNulls = false;

  private boolean indent = false;

  @Override
  public void close() {
  }

  @Override
  public void flush() {
  }

  public boolean isIndent() {
    return this.indent;
  }

  public boolean isWritable(final Object value) {
    return com.revolsys.util.Property.hasValue(value) || isWriteNulls();
  }

  public boolean isWriteNulls() {
    return this.writeNulls;
  }

  public void setIndent(final boolean indent) {
    this.indent = indent;
  }

  public void setWriteNulls(final boolean writeNulls) {
    this.writeNulls = writeNulls;
  }
}
