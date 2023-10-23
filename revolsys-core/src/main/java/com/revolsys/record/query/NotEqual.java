package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;

public class NotEqual extends BinaryCondition {

  public NotEqual(final QueryValue left, final QueryValue right) {
    super(left, "<>", right);
  }

  @Override
  public NotEqual clone() {
    return (NotEqual)super.clone();
  }

  @Override
  public NotEqual newCondition(final QueryValue left, final QueryValue right) {
    return new NotEqual(left, right);
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getLeft();
    final Object value1 = left.getValue(record);

    final QueryValue right = getRight();
    final Object value2 = right.getValue(record);

    return !DataType.equal(value1, value2);
  }
}
