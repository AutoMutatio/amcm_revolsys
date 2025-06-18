package com.revolsys.record.query;

import com.revolsys.record.schema.RecordStore;

public class Any extends AbstractArrayExpression {

  public Any(final QueryValue value) {
    super(value);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("ANY(");
    super.appendDefaultSql(statement, recordStore, buffer);
    buffer.append(")");
  }

  @Override
  public Any clone() {
    return (Any)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Any) {
      return super.equals(obj);
    }
    return false;
  }

  @Override
  public String toString() {
    return "ANY(" + super.toString() + ")";
  }
}
