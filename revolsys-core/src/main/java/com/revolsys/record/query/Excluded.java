package com.revolsys.record.query;

public class Excluded extends ColumnWithPrefix {
  public Excluded(final ColumnReference column) {
    super("EXCLUDED", column);
  }
}
