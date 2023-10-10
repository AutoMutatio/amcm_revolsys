package com.revolsys.record.query.functions;

import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.query.QueryValue;
import com.revolsys.util.Property;

public class StartsWith extends BinaryFunction {

  public static final String NAME = "starts_with";

  public StartsWith(final List<QueryValue> parameters) {
    super(NAME, parameters);
  }

  public StartsWith(final QueryValue left, final QueryValue right) {
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
      return (V)(Boolean)left.toUpperCase().startsWith(right.toUpperCase());
    } else {
      return (V)Boolean.FALSE;
    }
  }

}
