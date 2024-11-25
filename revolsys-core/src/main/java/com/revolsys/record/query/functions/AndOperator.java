package com.revolsys.record.query.functions;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.query.AbstractBinaryQueryValue;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordStore;

public class AndOperator extends AbstractBinaryQueryValue {

  public AndOperator(final QueryValue left, final QueryValue right) {
    super(left, right);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append(")");
    appendLeft(statement, recordStore, buffer);
    buffer.append(" AND ");
    appendRight(statement, recordStore, buffer);
    buffer.append(")");
  }

  @Override
  public AndOperator clone() {
    return (AndOperator)super.clone();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof final AndOperator andOperator) {
      return super.equals(andOperator);
    }
    return false;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    throw new UnsupportedOperationException("getValue");
  }

  @Override
  public String toString() {
    return "(" + getLeft().toString() + " AND " + getRight().toString() + ")";
  }
}
