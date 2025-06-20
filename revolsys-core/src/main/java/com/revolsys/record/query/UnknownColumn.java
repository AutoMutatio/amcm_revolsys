package com.revolsys.record.query;

public class UnknownColumn extends Column {

  public UnknownColumn(final CharSequence name) {
    super(name);
  }

  @Override
  public void appendColumnName(final SqlAppendable string) {
    string.append("NULL");
  }
}
