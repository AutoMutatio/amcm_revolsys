package com.revolsys.record.query;

import com.revolsys.exception.Exceptions;

public class OrderBy implements Cloneable {
  private QueryValue field;

  private boolean ascending;

  private String collate;

  private Boolean nullsFirst;

  public OrderBy(final QueryValue field, final boolean ascending) {
    this.field = field;
    this.ascending = ascending;
  }

  public void appendSql(final QueryStatement statement, final TableReferenceProxy table,
    final SqlAppendable sql) {
    table.getTableReference()
      .appendSelect(statement, sql, this.field);
    if (!this.ascending) {
      sql.append(" desc");
    }
    if (this.nullsFirst != null) {
      sql.append(" nulls ");
      if (this.nullsFirst) {
        sql.append("first");
      } else {
        sql.append("last");
      }
    }

    if (this.collate != null) {
      sql.append(" collate ");
      sql.append(this.collate);
    }
  }

  public OrderBy ascending(final boolean ascending) {
    this.ascending = ascending;
    return this;
  }

  @Override
  protected OrderBy clone() {
    try {
      return (OrderBy)super.clone();
    } catch (final CloneNotSupportedException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  public OrderBy collate(final String collate) {
    this.collate = collate;
    return this;
  }

  public QueryValue getField() {
    return this.field;
  }

  public boolean isAscending() {
    return this.ascending;
  }

  public boolean isField(final String fieldName) {
    if (this.field instanceof final ColumnReference column) {
      if (column.getName()
        .equalsIgnoreCase(fieldName)) {
        return true;
      }
    }
    return false;
  }

  public OrderBy nullsFirst(final boolean nullsFirst) {
    this.nullsFirst = nullsFirst;
    return this;
  }

  public OrderBy withField(final QueryValue field) {
    final OrderBy clone = clone();
    clone.field = field;
    return clone;
  }
}
