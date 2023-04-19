package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.record.schema.RecordStore;

public class NoCondition implements Condition {

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return 0;
  }

  @Override
  public NoCondition clone() {
    return this;
  }

  @Override
  public Condition clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public boolean isEmpty() {
    return true;
  }

}
