package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.comparator.CompareUtil;

public class GreaterThan extends BinaryCondition {

  public GreaterThan(final QueryValue left, final QueryValue right) {
    super(left, ">", right);
  }

  @Override
  public GreaterThan clone() {
    return (GreaterThan)super.clone();
  }

  @Override
  public GreaterThan newCondition(final QueryValue left, final QueryValue right) {
    return new GreaterThan(left, right);
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getLeft();
    final Object value1 = left.getValue(record);

    final QueryValue right = getRight();
    final Object value2 = right.getValue(record);

    return CompareUtil.compare(value1, value2) > 0;
  }

}
