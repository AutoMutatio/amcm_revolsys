package com.revolsys.record.schema;

import java.util.function.Function;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.record.Record;
import com.revolsys.record.query.InsertStatement;

public class TableRecordStoreInsertStatement extends InsertStatement {
  private final TableRecordStoreConnection connection;

  private final AbstractTableRecordStore recordStore;

  public TableRecordStoreInsertStatement(final AbstractTableRecordStore recordStore,
    final TableRecordStoreConnection connection) {
    this.recordStore = recordStore;
    this.connection = connection;
  }

  @Override
  public int executeInsertCount() {
    return this.connection.transactionCall(super::executeInsertCount);
  }

  @Override
  public Record executeInsertRecord() {
    return this.connection.transactionCall(super::executeInsertRecord);
  }

  @Override
  public <V> V executeInsertRecords(final Function<BaseIterable<Record>, V> action) {
    return super.executeInsertRecords(records -> {
      final var mappedRecords = records.map(record -> {
        this.recordStore.insertStatementRecordAfter(this.connection, record);
        return record;
      });
      return action.apply(mappedRecords);
    });
  }
}
