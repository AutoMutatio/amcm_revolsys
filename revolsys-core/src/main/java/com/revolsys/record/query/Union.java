package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Map;

public class Union {

  private final Query query;

  private boolean distinct = true;

  Union(final Query query, final boolean distinct) {
    this.query = query;
    this.distinct = distinct;
  }

  public int appendParameters(final int index, Map<String, Object> parameters,
    final PreparedStatement statement) {
    return this.query.appendParameters(index, parameters, statement);
  }

  void appendSql(final SqlAppendable sql) {
    sql.append(" UNION ");
    if (!this.distinct) {
      sql.append("ALL ");
    }
    sql.append('(');
    this.query.appendSql(sql);
    sql.append(')');
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    appendSql(sql);
    return sql.toSqlString();
  }
}
