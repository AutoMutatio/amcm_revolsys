package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.schema.RecordStore;

public record JdbcFieldPlaceholder(String key, JdbcFieldDefinition field) implements QueryValue {

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    this.field.addSelectStatementPlaceHolder(sql);
  }

  @Override
  public int appendParameters(final int index, Map<String, Object> parameters,
    final PreparedStatement statement) {
    final var value = parameters.get(this.key);
    try {
      return this.field.setPreparedStatementValue(statement, index, value);
    } catch (final SQLException e) {
      throw Exceptions.wrap("Unable to set value", e)
        .property("value", value);
    }
  }

  @Override
  public JdbcFieldPlaceholder clone() {
    try {
      return (JdbcFieldPlaceholder)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public QueryValue clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }

  @Override
  public String toString() {
    return "?";
  }
}
