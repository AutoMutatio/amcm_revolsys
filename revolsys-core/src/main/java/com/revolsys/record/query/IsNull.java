package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;

public class IsNull extends RightUnaryCondition {

  public IsNull(final QueryValue value) {
    super(value, "IS NULL");
  }

  @Override
  public void appendOData(final StringBuilder s) {
    getValue().appendOData(s);
    s.append(" eq null");
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue queryValue = getValue();
    final Object value = queryValue.getValue(record);
    return value == null;
  }
}
