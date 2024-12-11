package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;

public class IsNotDistinctFrom extends BinaryCondition {

  public IsNotDistinctFrom(final QueryValue left, final QueryValue right) {
    super(left, "IS DISTINCT FROM", right);
  }

  @Override
  public IsNotDistinctFrom clone() {
    return (IsNotDistinctFrom)super.clone();
  }

  @Override
  public IsNotDistinctFrom newCondition(final QueryValue left, final QueryValue right) {
    return new IsNotDistinctFrom(left, right);
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getLeft();
    final Object value1 = left.getValue(record);

    final QueryValue right = getRight();
    final Object value2 = right.getValue(record);

    return DataType.equal(value1, value2);
  }
}
