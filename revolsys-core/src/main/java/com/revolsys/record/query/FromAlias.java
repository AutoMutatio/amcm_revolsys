package com.revolsys.record.query;

import com.revolsys.record.schema.FieldDefinition;

public class FromAlias implements From {
  private final From from;

  private final String alias;

  public FromAlias(final From from, final String alias) {
    this.from = from;
    this.alias = alias;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    this.from.appendFrom(sql);
  }

  @Override
  public void appendFromWithAlias(final SqlAppendable sql) {
    appendFrom(sql);
    sql.append(" ");
    sql.append(this.alias);
  }

  @Override
  public ColumnReference getColumn(final CharSequence name) {
    return new ColumnWithPrefix(this.alias, this.from.getColumn(name));
  }

  @Override
  public FieldDefinition getField(final CharSequence name) {
    return this.from.getField(name);
  }

  @Override
  public String getTableAlias() {
    return this.alias;
  }

  @Override
  public TableReference getTableReference() {
    return this.from.getTableReference();
  }
}
