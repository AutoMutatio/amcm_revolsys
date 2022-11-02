package com.revolsys.record.query;

public class FromSql implements From {

  private final String from;

  public FromSql(final String from) {
    this.from = from;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    sql.append('(');
    sql.append(this.from);
    sql.append(')');
  }
}
