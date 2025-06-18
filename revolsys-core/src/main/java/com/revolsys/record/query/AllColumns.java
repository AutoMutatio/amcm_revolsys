package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordStore;

public class AllColumns implements QueryValue {

  private TableReference table;

  public AllColumns() {
  }

  public AllColumns(final TableReference table) {
    this.table = table;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (this.table != null) {
      this.table.appendColumnPrefix(sql);
    }
    sql.append("*");
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  protected void appendValue(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendDefaultSql(statement, recordStore, sql);
  }

  @Override
  public AllColumns clone(final TableReference oldTable, final TableReference newTable) {
    try {
      return (AllColumns)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof AllColumns) {
      final AllColumns allColumns = (AllColumns)obj;
      if (DataType.equal(allColumns.getTable(), getTable())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public List<QueryValue> getQueryValues() {
    return Collections.emptyList();
  }

  @Override
  public String getStringValue(final MapEx record) {
    return null;
  }

  public TableReference getTable() {
    return this.table;
  }

  public <V extends QueryValue> V getValue() {
    return null;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable string = SqlAppendable.stringBuilder();
    if (this.table != null) {
      this.table.appendColumnPrefix(string);
    }
    string.append('*');
    return string.toString();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <QV extends QueryValue> QV updateQueryValues(final TableReference oldTable,
    final TableReference newTable, final Function<QueryValue, QueryValue> valueHandler) {
    return (QV)this;
  }
}
