package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.record.schema.RecordStore;

public record SetClause(ColumnReference column, QueryValue value) implements SqlAppendParameters {

  public static void appendSet(final SqlAppendable sql, final RecordStore recordStore,
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
        set.appendSql(recordStore, sql);
      }
    }
  }

  public void appendSql(final RecordStore recordStore, final SqlAppendable sql) {
    this.column.appendColumnName(sql);
    sql.append(" = ");
    if (this.value == null) {
      sql.append("null");
    } else {
      this.value.appendSql(null, recordStore, sql);
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
