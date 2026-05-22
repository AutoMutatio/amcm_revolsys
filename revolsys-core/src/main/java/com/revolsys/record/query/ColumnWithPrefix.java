package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class ColumnWithPrefix implements QueryValue, ColumnReference {

  private final String columnPrefix;

  private final QueryValue column;

  public ColumnWithPrefix(final CharSequence columnPrefix, final QueryValue column) {
    if (columnPrefix == null) {
      this.columnPrefix = null;
    } else {
      this.columnPrefix = columnPrefix.toString();
    }
    this.column = column;
  }

  @Override
  public void appendColumnName(final SqlAppendable string) {
    getColumn().appendColumnName(string);
  }

  @Override
  public void appendColumnPrefix(final SqlAppendable string) {
    if (this.columnPrefix != null) {
      string.append('"');
      string.append(this.columnPrefix);
      string.append('"');
      string.append(".");
    }
  }

  @Override
  public void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendColumnNameWithPrefix(sql);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendColumnNameWithPrefix(sql);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.column.appendParameters(index, statement);
  }

  @Override
  public ColumnWithPrefix clone() {
    try {
      return (ColumnWithPrefix)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public ColumnWithPrefix clone(final TableReference oldTable, final TableReference newTable) {
    if (oldTable != newTable) {
      final QueryValue clonedColumn = this.column.clone(oldTable, newTable);
      return new ColumnWithPrefix(this.columnPrefix, clonedColumn);
    }
    return clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ColumnWithPrefix) {
      final ColumnWithPrefix columnWithPrefix = (ColumnWithPrefix)obj;
      if (this.column.equals(columnWithPrefix.column)) {
        return DataType.equal(columnWithPrefix.getName(), getName());
      }
    }
    return false;
  }

  @Override
  public ColumnReference getColumn() {
    return this.column.getColumn();
  }

  @Override
  public FieldDefinition getFieldDefinition() {
    return getColumn().getFieldDefinition();
  }

  @Override
  public int getFieldIndex() {
    return getColumn().getFieldIndex();
  }

  @Override
  public String getName() {
    return getColumn().getName();
  }

  @Override
  public String getStringValue(final MapEx record) {
    final Object value = getValue(record);
    return getColumn().toString(value);
  }

  @Override
  public TableReferenceProxy getTable() {
    return getColumn().getTable();
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getValue(final MapEx record) {
    if (record == null) {
      return null;
    } else {
      final String name = getName();
      return (V)record.getValue(name);
    }
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return getColumn().getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings, null);
  }

  @Override
  public QueryValue toAlias(final String alias) {
    // Make sure the column knows the alias
    this.column.toAlias(alias);
    return ColumnReference.super.toAlias(alias);
  }

  @Override
  public <V> V toColumnTypeException(final Object value) {
    if (value == null) {
      return null;
    } else {
      return getColumn().toColumnTypeException(value);
    }
  }

  @Override
  public <V> V toFieldValueException(final Object value) {
    if (value == null) {
      return null;
    } else {
      return getColumn().toFieldValueException(value);
    }
  }

  @Override
  public <V> V toFieldValueException(final RecordState state, final Object value) {
    if (value == null) {
      return null;
    } else {
      return getColumn().toFieldValueException(state, value);
    }
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    appendColumnNameWithPrefix(sql);
    return sql.toSqlString();
  }

  @Override
  public String toString(final Object value) {
    return getColumn().toString(value);
  }
}
