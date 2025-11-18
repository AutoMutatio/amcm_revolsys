package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
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
  public TableRecordStoreQuery clone() {
    return (TableRecordStoreQuery)super.clone();
  }

  @Override
  public int deleteRecords() {
    return transactionCall(() -> this.recordStore.getRecordStore()
      .deleteRecords(this));
  }

  @Override
  public boolean exists() {
    return this.recordStore.exists(this.connection, this);
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

  @SuppressWarnings("unchecked")
  public <RS extends AbstractTableRecordStore> RS getTableRecordStore() {
    return (RS)this.recordStore;
  }

  @SuppressWarnings("unchecked")
  public <RS extends AbstractTableRecordStore> RS getTableRecordStore(final CharSequence name) {
    return (RS)this.connection.getTableRecordStore(name);
  }

  @Override
  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    return transactionCall(
      () -> this.recordStore.insertRecord(this.connection, this, newRecordSupplier));
  }

  @Override
  public Record newRecord() {
    return this.recordStore.newRecord(this.connection);
  }

  public TableRecordStoreQuery selectVirtual(final Iterable<Object> fields) {
    for (final var field : fields) {
      this.recordStore.addSelect(this.connection, this, field);
    }
    return this;
  }

  public TableRecordStoreQuery selectVirtual(final String... columnNames) {
    for (final var columnName : columnNames) {
      this.recordStore.addSelect(this.connection, this, columnName);
    }
    return this;
  }

  @Override
  public QueryValue newSelectClause(final Object select) {
    if (select instanceof final String str) {
      return this.recordStore.fieldPathToQueryValue(this, str);
    } else {
      return super.newSelectClause(select);
    }
  }

  @Override
  public TransactionBuilder transaction() {
    return this.connection.transaction();
  }

  @Override
  public Record updateRecord(final Consumer<Record> updateAction) {
    return this.recordStore.updateRecord(this.connection, this, updateAction);
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return transactionCall(
      () -> this.recordStore.updateRecords(this.connection, this, updateAction));
  }

}
