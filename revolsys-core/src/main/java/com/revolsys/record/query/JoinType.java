package com.revolsys.record.query;

public enum JoinType {

  CROSS_JOIN, INNER_JOIN, LEFT_OUTER_JOIN, RIGHT_OUTER_JOIN, FULL_OUTER_JOIN, COMMA(", ");

  public static final JoinType JOIN = INNER_JOIN;

  private final String sql;

  private JoinType() {
    this.sql = name().replace('_', ' ');
  }

  private JoinType(final String sql) {
    this.sql = sql;
  }

  public String getSql() {
    return this.sql;
  }

  @Override
  public String toString() {
    return this.sql;
  }
}
