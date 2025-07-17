package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.schema.RecordStore;

public class StringValue implements QueryValue {

  private final String string;

  public StringValue(final String string) {
    this.string = string;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append('?');
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    try {
      statement.setString(index, this.string);
      return index + 1;
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  public StringValue clone() {
    try {
      return (StringValue)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public QueryValue clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof final StringValue sl) {
      return DataType.equal(sl.string, this.string);
    } else {
      return false;
    }
  }

  public String getString() {
    return this.string;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {
    return (V)this.string;
  }

  @Override
  public String toString() {
    return "'" + this.string + "'";
  }
}
