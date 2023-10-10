package com.revolsys.record.query;

import com.revolsys.record.schema.RecordStore;

public class All extends AbstractArrayExpression {

  public All(final QueryValue value) {
    super(value);
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("ALL(");
    super.appendDefaultSql(query, recordStore, buffer);
    buffer.append(")");
  }

  @Override
  public All clone() {
    return (All)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof All) {
      return super.equals(obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return "ALL(" + super.toString() + ")";
  }
}
