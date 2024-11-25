package com.revolsys.record.query;

import java.sql.PreparedStatement;

public class With extends FromAlias implements SqlAppendParameters {

  private Query query;

  private boolean recursive = false;

  public With() {
    super();
  }

  public With(final String name, final Query query) {
    this(name, query, false);
  }

  public With(final String name, final Query query, final boolean recursive) {
    super(query, name);
    this.query = query;
    this.recursive = recursive;
  }

  public String alias() {
    return getTableAlias();
  }

  @Override
  public With alias(final String alias) {
    super.alias(alias);
    return this;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    final var alias = alias();
    sql.append(alias);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.query.appendParameters(index, statement);
  }

  public void appendSql(final SqlAppendable sql) {
    if (this.recursive) {
      sql.append("RECURSIVE ");
    }
    sql.append(getTableAlias());
    sql.append(" AS (");
    this.query.appendSql(sql);
    sql.append(')');

  }

  public boolean isRecursive() {
    return this.recursive;
  }

  public Query query() {
    return this.query;
  }

  public With query(final Query query) {
    this.query = query;
    from(query);
    return this;
  }

  @Override
  public String toString() {
    final var sql = new StringBuilderSqlAppendable();
    appendSql(sql);
    return sql.toString();
  }
}
