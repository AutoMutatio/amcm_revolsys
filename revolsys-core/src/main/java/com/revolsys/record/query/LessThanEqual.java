package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.comparator.CompareUtil;

public class LessThanEqual extends BinaryCondition {

  public LessThanEqual(final QueryValue left, final QueryValue right) {
    super(left, "<=", right);
  }

  @Override
  public LessThanEqual clone() {
    return (LessThanEqual)super.clone();
  }

  @Override
  public LessThanEqual newCondition(final QueryValue left, final QueryValue right) {
    return new LessThanEqual(left, right);
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getLeft();
    final Object value1 = left.getValue(record);

    final QueryValue right = getRight();
    final Object value2 = right.getValue(record);

    return CompareUtil.compare(value1, value2) <= 0;
  }

}
