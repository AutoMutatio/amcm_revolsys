package com.revolsys.gis.esri.gdb.file.capi.type;

import com.revolsys.data.type.DataTypes;
import com.revolsys.esri.filegdb.jni.Row;
import com.revolsys.geometry.model.Geometry;
import com.revolsys.record.Record;
import com.revolsys.record.io.format.esri.gdb.xml.model.Field;
import com.revolsys.util.Booleans;

public class LengthFieldDefinition extends AbstractFileGdbFieldDefinition {
  public LengthFieldDefinition(final int fieldNumber, final Field field) {
    super(fieldNumber, field.getName(), DataTypes.DOUBLE,
      Booleans.getBoolean(field.getRequired()) || !field.isIsNullable());
  }

  @Override
  public int getMaxStringLength() {
    return 19;
  }

  @Override
  public Object getValue(final Row row) {
    synchronized (row) {
      if (row.isNull(this.fieldNumber)) {
        return null;
      } else {
        return row.getDouble(this.fieldNumber);
      }
    }
  }

  @Override
  public boolean isAutoCalculated() {
    return true;
  }

  @Override
  public void setValue(final Record record, final Row row, final Object value) {
    double length = 0;
    final Geometry geometry = record.getGeometry();
    if (geometry != null) {
      length = geometry.getLength();
    }
    row.setDouble(this.fieldNumber, length);
  }
}
