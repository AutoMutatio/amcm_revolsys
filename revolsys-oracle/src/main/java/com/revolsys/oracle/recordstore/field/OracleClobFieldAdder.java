package com.revolsys.oracle.recordstore.field;

import com.revolsys.jdbc.field.JdbcFieldAdder;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcStringFieldDefinition;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;

public class OracleClobFieldAdder extends JdbcFieldAdder {

  public OracleClobFieldAdder() {
  }

  @Override
  public JdbcFieldDefinition newField(final AbstractJdbcRecordStore recordStore,
    final JdbcRecordDefinition recordDefinition, final String dbName, final String name,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description) {
    if (recordStore.isLobAsString() || recordStore.isClobAsString()) {
      return new JdbcStringFieldDefinition(dbName, name, sqlType, dbDataType, 0, required,
        description, null);
    } else {
      return new OracleJdbcClobFieldDefinition(dbName, name, sqlType, dbDataType, required,
        description);
    }
  }
}
