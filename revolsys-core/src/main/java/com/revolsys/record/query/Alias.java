package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class Alias implements QueryValue {

  private final String alias;

  private final QueryValue value;

  public Alias(final QueryValue values, final CharSequence alias) {
    this.value = values;
    this.alias = alias.toString();
  }

  protected void appendAlias(final SqlAppendable sql) {
    sql.append('"');
    sql.append(this.alias);
    sql.append('"');
  }

  @Override
  public void appendDefaultSelect(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    this.value.appendDefaultSelect(query, recordStore, sql);
    sql.append(" as ");
    appendAlias(sql);
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(this.alias);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  @Override
  public Alias clone() {
    try {
      return (Alias)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public Alias clone(final TableReference oldTable, final TableReference newTable) {
    if (oldTable != newTable) {
      final QueryValue clonedValue = this.value.clone(oldTable, newTable);
      return new Alias(clonedValue, this.alias);
    }
    return clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Alias) {
      final Alias alias = (Alias)obj;
      if (this.value.equals(alias.value)) {
        return DataType.equal(alias.alias, alias);
      }
    }
    return false;
  }

  @Override
  public int getFieldIndex() {
    return this.value.getFieldIndex();
  }

  @Override
  public String getStringValue(final MapEx record) {
    return this.value.getStringValue(record);
  }

  public QueryValue getValue() {
    return this.value;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getValue(final MapEx record) {
    if (record == null) {
      return null;
    } else {
      return (V)this.value.getValue(record);
    }
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return this.value.getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    this.value.appendDefaultSelect(new Query(), null, sql);
    sql.append(" as ");
    appendAlias(sql);
    return sql.toSqlString();
  }

}
