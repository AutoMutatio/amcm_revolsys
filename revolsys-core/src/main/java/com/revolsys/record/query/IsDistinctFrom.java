package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;

public class IsDistinctFrom extends BinaryCondition {

  public IsDistinctFrom(final QueryValue left, final QueryValue right) {
    super(left, "IS NOT DISTINCT FROM", right);
  }

  @Override
  public IsDistinctFrom clone() {
    return (IsDistinctFrom)super.clone();
  }

  @Override
  public IsDistinctFrom newCondition(final QueryValue left, final QueryValue right) {
    return new IsDistinctFrom(left, right);
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
