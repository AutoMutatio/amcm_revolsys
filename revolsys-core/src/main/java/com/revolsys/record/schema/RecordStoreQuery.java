package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.StringBuilderSqlAppendable;
import com.revolsys.record.query.TableReferenceProxy;
import com.revolsys.transaction.TransactionBuilder;

public class RecordStoreQuery extends Query {

  public RecordStoreQuery(final RecordStore recordStore) {
    super(recordStore);
  }

  public RecordStoreQuery(final RecordStore recordStore, final TableReferenceProxy table) {
    super(recordStore, table);
  }

  @Override
  public int deleteRecords() {
    return transactionCall(() -> getRecordStore().deleteRecords(this));
  }

  @Override
  public boolean exists() {
    return getRecordStore().exists(this);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends Record> R getRecord() {
    return transactionCall(() -> (R)getRecordStore().getRecord(this));
  }

  @Override
  public long getRecordCount() {
    return transactionCall(() -> getRecordStore().getRecordCount(this));
  }

  @Override
  public RecordReader getRecordReader() {
    return getRecordStore().getRecords(this);
  }

  @Override
  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    return transactionCall(() -> getRecordStore().insertRecord(this, newRecordSupplier));
  }

  @Override
  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = super.newSqlAppendable();
    sql.setRecordStore(getRecordStore());
    return sql;
  }

  @Override
  public TransactionBuilder transaction() {
    return getRecordStore().transaction();
  }

  @Override
  public Record updateRecord(final Consumer<Record> updateAction) {
    return transactionCall(() -> getRecordStore().updateRecord(this, updateAction));
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return transactionCall(() -> getRecordStore().updateRecords(this, updateAction));
  }
}
