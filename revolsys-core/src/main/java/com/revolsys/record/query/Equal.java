package com.revolsys.record.query;

import org.jeometry.common.collection.map.MapEx;
import org.jeometry.common.data.type.DataType;

public class Equal extends BinaryCondition {

  public Equal(final QueryValue left, final QueryValue right) {
    super(left, "=", right);
  }

  @Override
  public Equal clone() {
    return (Equal)super.clone();
  }

  @Override
  public Equal newCondition(final QueryValue left, final QueryValue right) {
    return new Equal(left, right);
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
