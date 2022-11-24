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

  @Override
  public ColumnReference getColumn(final CharSequence name) {
    return new Column(this, name);
  }

  @Override
  public TableReference getTableReference() {
    return null;
  }
}
