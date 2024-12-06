package com.revolsys.record.query.functions;

import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.query.AbstractUnaryQueryValue;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordStore;

public class JsonRemoveKey extends AbstractUnaryQueryValue implements Condition {

  private final String key;

  public JsonRemoveKey(final QueryValue left, final String key) {
    super(left);
    this.key = key;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    getValue().appendSql(statement, recordStore, sql);
    sql.append(" - ?");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    final var left = getValue();
    if (left != null) {
      index = left.appendParameters(index, statement);
    }
    try {
      statement.setString(index++, this.key);
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return index;
  }

  @Override
  public JsonRemoveKey clone() {
    return (JsonRemoveKey)super.clone();
  }

  @Override
  public JsonRemoveKey clone(final TableReference oldTable, final TableReference newTable) {
    return (JsonRemoveKey)super.clone(oldTable, newTable);
  }

  @Override
  public boolean test(final MapEx record) {
    return true;
  }

}
