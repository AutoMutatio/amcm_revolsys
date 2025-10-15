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

public class ColumnAlias implements QueryValue, ColumnReference {

  private final String alias;

  private final ColumnReference column;

  public ColumnAlias(final ColumnReference column, final CharSequence alias) {
    this.column = column;
    this.alias = alias.toString();
  }

  protected void appendAlias(final SqlAppendable sql) {
    sql.append('"');
    sql.append(this.alias);
    sql.append('"');
  }

  @Override
  public void appendColumnName(final SqlAppendable string) {
    this.column.appendColumnName(string);
  }

  @Override
  public void appendColumnPrefix(final SqlAppendable string) {
    this.column.appendColumnPrefix(string);
  }

  @Override
  public void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    this.column.appendDefaultSelect(statement, recordStore, sql);
    sql.append(" as ");
    appendAlias(sql);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(this.alias);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.column.appendParameters(index, statement);
  }

  @Override
  public ColumnAlias clone() {
    try {
      return (ColumnAlias)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public ColumnAlias clone(final TableReference oldTable, final TableReference newTable) {
    if (oldTable != newTable) {
      final ColumnReference clonedColumn = this.column.clone(oldTable, newTable);
      return new ColumnAlias(clonedColumn, this.alias);
    }
    return clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof ColumnAlias) {
      final ColumnAlias alias = (ColumnAlias)obj;
      if (this.column.equals(alias.column)) {
        return DataType.equal(alias.getName(), getName());
      }
    }
    return false;
  }

  @Override
  public String getAliasName() {
    return this.alias;
  }

  @Override
  public FieldDefinition getFieldDefinition() {
    return this.column.getFieldDefinition();
  }

  @Override
  public int getFieldIndex() {
    return this.column.getFieldIndex();
  }

  @Override
  public String getName() {
    return this.alias;
  }

  @Override
  public String getStringValue(final MapEx record) {
    final Object value = getValue(record);
    return this.column.toString(value);
  }

  @Override
  public TableReferenceProxy getTable() {
    return this.column.getTable();
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
    return this.column.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings, this.alias);
  }

  @Override
  public <V> V toColumnTypeException(final Object value) {
    if (value == null) {
      return null;
    } else {
      return this.column.toColumnTypeException(value);
    }
  }

  @Override
  public <V> V toFieldValueException(final Object value) {
    if (value == null) {
      return null;
    } else {
      return this.column.toFieldValueException(value);
    }
  }

  @Override
  public <V> V toFieldValueException(final RecordState state, final Object value) {
    if (value == null) {
      return null;
    } else {
      return this.column.toFieldValueException(state, value);
    }
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    this.column.appendColumnNameWithPrefix(sql);
    sql.append(" as ");
    appendAlias(sql);
    return sql.toSqlString();
  }

  @Override
  public String toString(final Object value) {
    return this.column.toString(value);
  }
}
