package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public class NullValue implements QueryValue {

  public static final NullValue INSTANCE = new NullValue();

  private NullValue() {
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append("NULL");
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  @Override
  public QueryValue clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }
}
