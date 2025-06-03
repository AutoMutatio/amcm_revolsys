package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.record.schema.FieldDefinition;

public class FromAlias implements From {
  private From from;

  private String alias;

  protected FromAlias() {
  }

  public FromAlias(final From from, final String alias) {
    this.from = from;
    this.alias = alias;
  }

  protected FromAlias alias(final String alias) {
    this.alias = alias;
    return this;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    this.from.appendFrom(sql);
  }

  @Override
  public int appendFromParameters(final int index, final PreparedStatement statement) {
    return this.from.appendFromParameters(index, statement);
  }

  @Override
  public void appendFromWithAlias(final SqlAppendable sql) {
    appendFrom(sql);
    sql.append(" ");
    sql.append(this.alias);
  }

  protected FromAlias from(final From from) {
    this.from = from;
    return this;
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
