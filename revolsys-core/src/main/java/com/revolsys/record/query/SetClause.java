package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.iterator.BaseIterable;

public record SetClause(QueryStatement statement, ColumnReference column, QueryValue value)
  implements SqlAppendParameters {

  public static void appendSet(final SqlAppendable sql, final QueryStatement statement,
    final BaseIterable<SetClause> clauses) {
    sql.append(" SET ");
    {
      boolean first = true;
      for (final SetClause set : clauses) {
        if (first) {
          first = false;
        } else {
          sql.append(", ");
        }
        set.appendSql(statement, sql);
      }
    }
  }

  public void appendSql(final QueryStatement statement, final SqlAppendable sql) {
    this.column.appendColumnName(sql);
    sql.append(" = ");
    if (this.value == null) {
      sql.append("null");
    } else {
      this.value.appendSql(statement, sql);
    }
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    index = this.column.appendParameters(index, statement);
    if (this.value != null) {
      index = this.value.appendParameters(index, statement);
    }
    return index;
  }
}
