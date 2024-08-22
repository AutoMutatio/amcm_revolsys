package com.revolsys.record.query;

import com.revolsys.exception.Exceptions;

public class OrderBy implements Cloneable {
  private QueryValue field;

  private boolean ascending;

  private String collate;

  public OrderBy(final QueryValue field, final boolean ascending) {
    this.field = field;
    this.ascending = ascending;
  }

  public void appendSql(final Query query, final TableReferenceProxy table,
    final SqlAppendable sql) {
    table.getTableReference().appendSelect(query, sql, this.field);
    if (!this.ascending) {
      sql.append(" desc");
    }

    if (this.collate != null) {
      sql.append(" collate ");
      sql.append(this.collate);
    }
  }

  @Override
  protected OrderBy clone() {
    try {
      return (OrderBy)super.clone();
    } catch (final CloneNotSupportedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public QueryValue getField() {
    return this.field;
  }

  public boolean isAscending() {
    return this.ascending;
  }

  public boolean isField(final String fieldName) {
    if (this.field instanceof ColumnReference) {
      final ColumnReference column = (ColumnReference)this.field;
      if (column.getName().equalsIgnoreCase(fieldName)) {
        return true;
      }
    }
    return false;
  }

  public OrderBy setAscending(final boolean ascending) {
    this.ascending = ascending;
    return this;
  }

  public OrderBy setCollate(final String collate) {
    this.collate = collate;
    return this;
  }

  public OrderBy withField(final QueryValue field) {
    final OrderBy clone = clone();
    clone.field = field;
    return clone;
  }
}
