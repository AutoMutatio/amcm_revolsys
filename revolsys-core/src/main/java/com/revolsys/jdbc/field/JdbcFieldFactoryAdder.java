package com.revolsys.jdbc.field;

import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;

public class JdbcFieldFactoryAdder extends JdbcFieldAdder {
  private final JdbcFieldFactory factory;

  public JdbcFieldFactoryAdder(final JdbcFieldFactory factory) {
    this.factory = factory;
  }

  @Override
  public JdbcFieldDefinition newField(final AbstractJdbcRecordStore recordStore,
    final JdbcRecordDefinition recordDefinition, final String dbName, final String name,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description) {
    return (JdbcFieldDefinition)this.factory.newField(dbName, name, sqlType, dbDataType, length,
      scale, required, description, null);
  }
}
