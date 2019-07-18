package com.revolsys.fgdb.test.field;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jeometry.common.data.type.DataTypes;

public class DoubleField extends FgdbField {
  public DoubleField(final String name, final boolean required) {
    super(name, DataTypes.DOUBLE, required);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T read(final ByteBuffer buffer) throws IOException {
    final Double value = buffer.getDouble();
    return (T)value;
  }
}
