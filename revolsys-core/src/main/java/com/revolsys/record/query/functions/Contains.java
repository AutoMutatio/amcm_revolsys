package com.revolsys.record.query.functions;

import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.query.QueryValue;
import com.revolsys.util.Property;

public class Contains extends BinaryFunction {

  public static final String NAME = "contains";

  public Contains(final List<QueryValue> parameters) {
    super(NAME, parameters);
  }

  public Contains(final QueryValue left, final QueryValue right) {
    super(NAME, left, right);
  }

  @Override
  public String getStringValue(final MapEx record) {
    return getValue(record);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {
    final String left = getLeft().getStringValue(record);
    final String right = getRight().getStringValue(record);
    if (Property.hasValuesAll(left, right)) {
      return (V)(Boolean)left.toUpperCase()
        .contains(right.toUpperCase());
    } else {
      return (V)Boolean.FALSE;
    }
  }

}
