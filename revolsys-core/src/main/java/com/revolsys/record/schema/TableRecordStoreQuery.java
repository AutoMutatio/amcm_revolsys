package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.TransactionBuilder;

public class TableRecordStoreQuery extends Query {

  private final AbstractTableRecordStore recordStore;

  private final TableRecordStoreConnection connection;

  public TableRecordStoreQuery(final AbstractTableRecordStore recordStore,
    final TableRecordStoreConnection connection) {
    super(recordStore.getRecordDefinition());
    this.recordStore = recordStore;
    this.connection = connection;
  }

  @Override
  public int deleteRecords() {
    return transactionCall(() -> this.recordStore.getRecordStore().deleteRecords(this));
  }

  @Override
  public <R extends Record> R getRecord() {
    return transactionCall(() -> this.recordStore.getRecord(this.connection, this));
  }

  @Override
  public long getRecordCount() {
    return this.recordStore.getRecordCount(this.connection, this);
  }

  @Override
  public RecordReader getRecordReader() {
    return this.recordStore.getRecordReader(this.connection, this);
  }

  @Override
  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    return transactionCall(
      () -> this.recordStore.insertRecord(this.connection, this, newRecordSupplier));
  }

  @Override
  public Record newRecord() {
    return this.recordStore.newRecord();
  }

  @Override
  public TransactionBuilder transaction() {
    return this.connection.transaction();
  }

  @Override
  public Record updateRecord(final Consumer<Record> updateAction) {
    return transactionCall(
      () -> this.recordStore.updateRecord(this.connection, this, updateAction));
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return transactionCall(
      () -> this.recordStore.updateRecords(this.connection, this, updateAction));
  }

}
