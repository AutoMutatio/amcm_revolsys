package com.revolsys.record.query;

public class OnConflictDoNothing implements OnConflictAction {

  public static final OnConflictDoNothing INSTANCE = new OnConflictDoNothing();

  @Override
  public void appendSql(final SqlAppendable sql) {
    sql.append("DO NOTHING\n");
  }

}
