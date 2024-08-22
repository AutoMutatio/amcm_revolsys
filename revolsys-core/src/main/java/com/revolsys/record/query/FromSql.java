package com.revolsys.record.query;

import com.revolsys.record.schema.RecordStore;

public class FromSql implements From {

  private final String from;

  private RecordStore recordStore;

  private boolean useParenthesis;

  public FromSql(final String from) {
    this.from = from;
  }

  @Override
  public void appendFrom(final SqlAppendable sql) {
    if (this.useParenthesis) {
      sql.append('(');
    }
    sql.append(this.from);
    if (this.useParenthesis) {
      sql.append(')');
    }
  }

  @Override
  public ColumnReference getColumn(final CharSequence name) {
    return new Column(this, name);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <R extends RecordStore> R getRecordStore() {
    if (this.recordStore == null) {
      return From.super.getRecordStore();
    } else {
      return (R)this.recordStore;
    }
  }

  @Override
  public TableReference getTableReference() {
    return null;
  }

  public FromSql recordStore(final RecordStore recordStore) {
    this.recordStore = recordStore;
    return this;
  }

  public FromSql useParenthesis(final boolean useParenthesis) {
    this.useParenthesis = useParenthesis;
    return this;
  }
}
