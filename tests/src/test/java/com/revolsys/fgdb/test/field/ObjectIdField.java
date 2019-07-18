package com.revolsys.fgdb.test.field;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.jeometry.common.data.type.DataTypes;

import com.revolsys.record.Record;

public class ObjectIdField extends FgdbField {
  public ObjectIdField(final String name, final boolean required) {
    super(name, DataTypes.INT, required);
  }

  @Override
  public <T> T read(final ByteBuffer buffer) throws IOException {
    return null;
  }

  @Override
  public boolean setValue(final Record record, final ByteBuffer buffer) throws IOException {
    return false;
  }

  @Override
  public Object validate(final Object value) {
    return value;
  }
}
