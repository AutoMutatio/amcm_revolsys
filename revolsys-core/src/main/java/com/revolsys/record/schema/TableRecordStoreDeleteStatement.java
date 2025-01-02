package com.revolsys.record.schema;

import com.revolsys.record.query.DeleteStatement;

public class TableRecordStoreDeleteStatement extends DeleteStatement {
  private final TableRecordStoreConnection connection;

  public TableRecordStoreDeleteStatement(final TableRecordStoreConnection connection) {
    this.connection = connection;
  }
}
