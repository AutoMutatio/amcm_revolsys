package com.revolsys.record.schema;

import java.util.function.Consumer;

import com.revolsys.record.ArrayChangeTrackRecord;
import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.StringBuilderSqlAppendable;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOption;
import com.revolsys.transaction.TransactionOptions;
import com.revolsys.transaction.TransactionRecordReader;

public class RecordStoreQuery extends Query {

  private final RecordStore recordStore;

  public RecordStoreQuery(final RecordStore recordStore) {
    this.recordStore = recordStore;
  }

  @Override
  public int deleteRecords() {
    try (
      Transaction transaction = this.recordStore.newTransaction(TransactionOptions.REQUIRED)) {
      return this.recordStore.deleteRecords(this);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends Record> R getRecord() {
    return (R)this.recordStore.getRecord(this);
  }

  @Override
  public long getRecordCount() {
    return this.recordStore.getRecordCount(this);
  }

  @Override
  public RecordReader getRecordReader() {
    final Transaction transaction = this.recordStore.newTransaction(TransactionOptions.REQUIRED);
    final RecordReader reader = this.recordStore.getRecords(this);
    return new TransactionRecordReader(reader, transaction);
  }

  @Override
  public RecordReader getRecordReader(Transaction transaction) {
    if (transaction == null) {
      transaction = this.recordStore.newTransaction(TransactionOptions.REQUIRED);
    }
    final RecordReader reader = this.recordStore.getRecords(this);
    return new TransactionRecordReader(reader, transaction);
  }

  @Override
  public Record insertOrUpdateRecord(final Consumer<Record> insertAction,
    final Consumer<Record> updateAction) {
    try (
      Transaction transaction = newTransaction(TransactionOptions.REQUIRED)) {
      setRecordFactory(ArrayChangeTrackRecord.FACTORY);
      return super.insertOrUpdateRecord(insertAction, updateAction);
    }
  }

  @Override
  protected Record insertRecordDo(final Consumer<Record> action) {
    final Record newRecord = getRecordDefinition().newRecord();
    this.recordStore.insertRecord(newRecord);
    return newRecord;
  }

  @Override
  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = super.newSqlAppendable();
    sql.setRecordStore(this.recordStore);
    return sql;
  }

  @Override
  public Transaction newTransaction() {
    return this.recordStore.newTransaction();
  }

  @Override
  public Transaction newTransaction(final TransactionOption... options) {
    return this.recordStore.newTransaction(options);
  }

  @Override
  protected Record updateRecordDo(final Record record, final Consumer<Record> updateAction) {
    updateAction.accept(record);
    if (record.isModified()) {
      this.recordStore.updateRecord(record);
    }
    return record;
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return this.recordStore.updateRecords(this, updateAction);
  }
}
