package com.revolsys.record.schema;

import java.util.function.Consumer;
import java.util.function.Supplier;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.util.MultiValueMap;

import com.revolsys.record.ChangeTrackRecord;
import com.revolsys.record.Record;
import com.revolsys.record.io.RecordReader;
import com.revolsys.record.query.Q;
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

  public TableRecordStoreQuery addSelect(final HttpServletRequest request) {
    getTableRecordStore().addSelect(this.connection, request, this);
    return this;
  }

  public TableRecordStoreQuery andEqual(final MultiValueMap<String, String> id) {
    // Add a filter for each of the id field names and values
    // Must only match 1 record
    id.forEach((fieldName, values) -> {
      final var column = fieldPathToQueryValue(fieldName);
      final var value = values.get(0);
      and(column, Q.EQUAL, value);
    });
    return this;
  }

  @Override
  public TableRecordStoreQuery clone() {
    return (TableRecordStoreQuery)super.clone();
  }

  public TableRecordStoreConnection connection() {
    return this.connection;
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

  public QueryValue fieldPathToQueryValue(final CharSequence path) {
    return getTableRecordStore().fieldPathToQueryValue(this, path);
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

  @Override
  public QueryValue newSelectClause(final Object select) {
    if (select instanceof final CharSequence str) {
      return this.recordStore.fieldPathToQueryValue(this, str);
    } else {
      return super.newSelectClause(select);
    }
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
