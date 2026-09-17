package com.revolsys.record.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class Call extends AbstractUnaryQueryValue {

  public Call(final QueryValue queryValue) {
    super(queryValue);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("CALL ");
    super.appendDefaultSql(statement, recordStore, buffer);
  }

  @Override
  public Call clone() {
    return (Call)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof final Call call) {
      return super.equals(call);
    }
    return false;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return null;
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("CALL ");
    buffer.append(super.toString());
    return buffer.toString();
  }
}
