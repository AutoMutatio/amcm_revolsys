package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.StringBuilderSqlAppendable;

public class RecordStoreQuery extends Query {

  private final RecordStore recordStore;

  public RecordStoreQuery(final RecordStore recordStore) {
    this.recordStore = recordStore;
  }

  @Override
  public int deleteRecords() {
    return this.recordStore.transactionCall(() -> this.recordStore.deleteRecords(this));
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
    return this.recordStore.getRecords(this);
  }

  @Override
  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    return this.recordStore.insertRecord(this, newRecordSupplier);
  }

  @Override
  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = super.newSqlAppendable();
    sql.setRecordStore(this.recordStore);
    return sql;
  }

  @Override
  public Record updateRecord(final Consumer<Record> updateAction) {
    return this.recordStore.updateRecord(this, updateAction);
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    return this.recordStore.updateRecords(this, updateAction);
  }
}
