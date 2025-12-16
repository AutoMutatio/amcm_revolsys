package com.revolsys.test.fgdb.test.field;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.revolsys.data.type.DataTypes;

public class ShortField extends FgdbField {
  public ShortField(final String name, final boolean required) {
    super(name, DataTypes.SHORT, required);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T read(final ByteBuffer buffer) throws IOException {
    final Short value = buffer.getShort();
    return (T)value;
  }
}
