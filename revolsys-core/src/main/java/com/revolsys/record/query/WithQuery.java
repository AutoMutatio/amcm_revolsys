package com.revolsys.record.query;

public class WithQuery {

  public String name;

  public Query query;

  public String alias;

  public void appendSql(final SqlAppendable sql) {
    sql.append(this.name);
    if (this.alias == null) {
      sql.append(" AS ");
      sql.append(this.alias);
    }
    // TODO column alias, column name
    sql.append(" AS ");
    // TODO MATERIALIZED
    sql.append('(');
    this.query.appendSql(sql);
    sql.append(')');
  }

  public String getAliasPrefix() {
    if (this.alias == null) {
      return this.name;
    } else {
      return this.alias;
    }
  }

  public ColumnReference getColumn(final CharSequence name) {
    ColumnReference tableColumn = this.query.getTable().getColumn(name);
    if (tableColumn == null) {
      tableColumn = new Column(name);
    }
    return new ColumnWithPrefix(tableColumn, getAliasPrefix());
  }
}
