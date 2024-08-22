package com.revolsys.swing.map.layer.record;

import java.util.function.Consumer;
import java.util.function.Supplier;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Query;

public class RecordLayerQuery extends Query {

  private final AbstractRecordLayer layer;

  public RecordLayerQuery(final AbstractRecordLayer layer) {
    this.layer = layer;
  }

  @Override
  public int deleteRecords() {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends Record> R getRecord() {
    return (R)this.layer.getRecord(this);
  }

  @Override
  public long getRecordCount() {
    return this.layer.getRecordCount(this);
  }

  @Override
  public RecordReader getRecordReader() {
    return this.layer.getRecordReader(this);
  }

  @Override
  public Record insertRecord(final Supplier<Record> newRecordSupplier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Record updateRecord(final Consumer<Record> updateAction) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int updateRecords(final Consumer<? super ChangeTrackRecord> updateAction) {
    throw new UnsupportedOperationException();
  }
}
