package com.revolsys.record.schema;

import com.revolsys.record.query.InsertStatement;

public class TableRecordStoreInsertStatement extends InsertStatement {
  private final TableRecordStoreConnection connection;

  public TableRecordStoreInsertStatement(final TableRecordStoreConnection connection) {
    this.connection = connection;
  }

  @Override
  public int executeInsertCount() {
    return this.connection.transactionCall(super::executeInsertCount);
  }
}
