package com.revolsys.record.query;

public class FromAlias implements From {
  private final From from;

  private final String alias;

  public FromAlias(final From from, final String alias) {
    this.from = from;
    this.alias = alias;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    this.from.appendFrom(sql);
  }

  @Override
  public void appendFromWithAlias(final SqlAppendable sql) {
    appendFrom(sql);
    sql.append(" ");
    sql.append(this.alias);
  }
}
