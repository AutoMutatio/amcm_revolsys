package com.revolsys.record.schema;

import com.revolsys.record.query.UpdateStatement;

public class TableRecordStoreUpdateStatement extends UpdateStatement {
  private final TableRecordStoreConnection connection;

  public TableRecordStoreUpdateStatement(final TableRecordStoreConnection connection) {
    this.connection = connection;
  }

  @Override
  public int updateRecords() {
    return this.connection.transactionCall(super::updateRecords);
  }
}
