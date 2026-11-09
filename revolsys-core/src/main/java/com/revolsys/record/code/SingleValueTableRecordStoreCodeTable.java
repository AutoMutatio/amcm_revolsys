package com.revolsys.record.code;

import java.util.Map;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.AbstractTableRecordStore;
import com.revolsys.record.schema.TableRecordStoreConnection;
import com.revolsys.record.schema.TableRecordStoreQuery;

public class SingleValueTableRecordStoreCodeTable
  extends AbstractRecordStoreSingleValueCodeTable<SingleValueTableRecordStoreCodeTable> {

  private AbstractTableRecordStore recordStore;

  private TableRecordStoreConnection connection;

  public SingleValueTableRecordStoreCodeTable(final AbstractTableRecordStore recordStore,
    final TableRecordStoreConnection connection) {
    this.recordStore = recordStore;
    this.connection = connection;
  }

  public SingleValueTableRecordStoreCodeTable(final Map<String, ? extends Object> config) {
    setProperties(config);
  }

  @Override
  protected int calculateValueFieldLength() {
    return this.recordStore.getRecordDefinition()
      .getFieldLength(this.valueFieldName);
  }

  @Override
  public SingleValueTableRecordStoreCodeTable clone() {
    return super.clone();
  }

  @Override
  protected QueryValue fieldPathToSelect(final Query query, final String idFieldName) {
    return this.recordStore.fieldPathToSelect((TableRecordStoreQuery)query, idFieldName);
  }

  @Override
  public Query newQueryDo() {
    return this.recordStore//
      .newQuery(this.connection);
  }

  @Override
  protected Record recordByIdDo(Object id) {
    if (id instanceof final Identifier identifier) {
      id = identifier.getValue(0);
    }
    return this.recordStore.newQuery(this.connection)
      .and(this.idFieldName, id)
      .getRecord();
  }

  @Override
  public String toString() {
    return this.recordStore.getTablePath() + " " + getIdFieldName() + " " + this.valueFieldName;

  }

}
