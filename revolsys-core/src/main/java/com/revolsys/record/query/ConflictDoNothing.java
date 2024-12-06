package com.revolsys.record.query;

public class ConflictDoNothing implements ConflictAction {

  public static final ConflictDoNothing INSTANCE = new ConflictDoNothing();

  @Override
  public void appendSql(final SqlAppendable sql) {
    sql.append("DO NOTHING\n");
  }

}
