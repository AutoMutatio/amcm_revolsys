package com.revolsys.jdbc.field;

import com.revolsys.data.type.DataTypes;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;

public class JdbcStringFieldAdder extends JdbcFieldAdder {

  public JdbcStringFieldAdder() {
    super(DataTypes.STRING);
  }

  @Override
  public JdbcFieldDefinition newField(final AbstractJdbcRecordStore recordStore,
    final JdbcRecordDefinition recordDefinition, final String dbName, final String name,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description) {
    return new JdbcStringFieldDefinition(dbName, name, sqlType, dbDataType, length, required,
      description, null);
  }
}
