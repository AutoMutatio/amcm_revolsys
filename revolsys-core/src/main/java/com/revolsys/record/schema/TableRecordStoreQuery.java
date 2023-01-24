package com.revolsys.record.schema;

import java.util.function.Consumer;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOption;
import com.revolsys.transaction.TransactionOptions;

public class TableRecordStoreQuery extends Query {

  public static TableRecordStoreQuery create(final TableRecordStoreConnection connection,
    final AbstractTableRecordStore recordStore, final RecordAccessType accessType) {
    final RecordStoreSecurityPolicyForField securityPolicy = recordStore
      .getSecurityPolicyForField(connection, accessType);
    securityPolicy.enforceAccessAllowed();
    final RecordDefinition recordDefinition = securityPolicy.getRecordDefinition();
    return new TableRecordStoreQuery(connection, recordStore, recordDefinition);
  }

  private final AbstractTableRecordStore recordStore;

  private final TableRecordStoreConnection connection;

  public TableRecordStoreQuery(final TableRecordStoreConnection connection,
    final AbstractTableRecordStore recordStore) {
    super(recordStore.getRecordDefinition());
    this.recordStore = recordStore;
    this.connection = connection;
  }

  private TableRecordStoreQuery(final TableRecordStoreConnection connection,
    final AbstractTableRecordStore recordStore, final RecordDefinition recordDefinition) {
    super(recordDefinition);
    this.recordStore = recordStore;
    this.connection = connection;
  }

  @Override
  public int deleteRecords() {
    try (
      Transaction transaction = this.connection.newTransaction(TransactionOptions.REQUIRED)) {
      return this.recordStore.getRecordStore().deleteRecords(this);
    }
  }

  @Override
  public <R extends Record> R getRecord() {
    return this.recordStore.getRecord(this.connection, this);
  }

  @Override
  public long getRecordCount() {
    return this.recordStore.getRecordCount(this.connection, this);
  }

  @Override
  public RecordReader getRecordReader() {
    this.recordStore.enforceAccessTypeSecurityPolicy(this.connection, RecordAccessType.READ);
    return this.recordStore.getRecordReader(this.connection, this);
  }

  @Override
  public RecordReader getRecordReader(final Transaction transaction) {
    this.recordStore.enforceAccessTypeSecurityPolicy(this.connection, RecordAccessType.READ);
    return this.recordStore.getRecordReader(this.connection, this, transaction);
  }

  @Override
  public Record insertOrUpdateRecord(final Consumer<Record> insertAction,
    final Consumer<Record> updateAction) {
    try (
      Transaction transaction = this.connection.newTransaction(TransactionOptions.REQUIRED)) {
      setRecordFactory(this.recordStore.changeTrackRecordFactory);
      return super.insertOrUpdateRecord(insertAction, updateAction);
    }
  }

  @Override
  protected Record insertRecordDo(final Consumer<Record> action) {
    return this.recordStore.insertRecord(this.connection, action);
  }

  @Override
  public Transaction newTransaction() {
    return this.connection.newTransaction();
  }

  @Override
  public Transaction newTransaction(final TransactionOption... options) {
    return this.connection.newTransaction(options);
  }

  @Override
  protected Record updateRecordDo(final Record record, final Consumer<Record> updateAction) {
    return this.recordStore.updateRecordDo(this.connection, (RecordStoreChangeTrackRecord)record,
      updateAction);
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return this.recordStore.updateRecords(this.connection, this, updateAction);
  }
}
