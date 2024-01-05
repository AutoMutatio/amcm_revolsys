package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;

public class WithQuery {

  private String name;

  private Query query;

  private ListEx<String> columnNames = Lists.newArray();

  private boolean recursive;

  public int appendParameters(final int index, final PreparedStatement statement) {
    return this.query.appendParameters(index, statement);
  }

  public void appendSql(final SqlAppendable sql) {
    if (this.recursive) {
      sql.append("RECURSIVE ");
    }
    sql.append(this.name);
    if (!this.columnNames.isEmpty()) {
      sql.append(" (");
      var first = true;
      for (final String columnName : this.columnNames) {
        if (first) {
          first = false;
        } else {
          sql.append(',');
        }
        sql.append('"');
        sql.append(columnName);
        sql.append('"');
      }
      sql.append(")");
    }
    sql.append(" AS ");
    // TODO MATERIALIZED
    sql.append('(');
    this.query.appendSql(sql);
    sql.append(')');
  }

  public WithQuery columnNames(final String... names) {
    this.columnNames = Lists.newArray(names);
    return this;
  }

  public String getAliasPrefix() {
    return this.name;
  }

  public ColumnReference getColumn(final CharSequence name) {
    ColumnReference tableColumn = this.query.getTable().getColumn(name);
    if (tableColumn == null) {
      tableColumn = new Column(name);
    }
    return new ColumnWithPrefix(getAliasPrefix(), tableColumn);
  }

  public String name() {
    return this.name;
  }

  public WithQuery name(final String name) {
    this.name = name;
    return this;
  }

  public WithQuery query(final Query query) {
    this.query = query;
    return this;
  }

  public WithQuery recursive() {
    this.recursive = true;
    return this;
  }
}
