package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;

public record AliasColumnReference(String alias, ColumnReference baseColumn)
  implements ColumnReference {

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(this.alias);
  }

  @Override
  public AliasColumnReference clone() {
    return new AliasColumnReference(this.alias, this.baseColumn);
  }

  @Override
  public void appendColumnName(final SqlAppendable sql) {
    sql.append(this.alias);
  }

  @Override
  public ColumnReference clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public FieldDefinition getFieldDefinition() {
    return this.baseColumn.getFieldDefinition();
  }

  @Override
  public String getName() {
    return this.alias;
  }

  @Override
  public String getStringValue(final MapEx record) {
    return null;
  }

  @Override
  public TableReferenceProxy getTable() {
    return this.baseColumn.getTable();
  }

  @Override
  public <V> V toColumnTypeException(final Object value) {
    return this.baseColumn.toColumnTypeException(value);
  }

  @Override
  public <V> V toFieldValueException(final Object value) {
    return this.baseColumn.toFieldValueException(value);
  }

  @Override
  public <V> V toFieldValueException(final RecordState state, final Object value) {
    return this.baseColumn.toFieldValueException(state, value);
  }

  @Override
  public String toString(final Object value) {
    // TODO Auto-generated method stub
    return null;
  }

}
