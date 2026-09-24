package com.revolsys.record.query.functions;

import com.revolsys.record.query.QueryValue;

public class ArrayRemove extends SimpleFunction {
  public ArrayRemove(QueryValue array, QueryValue value) {
    super("array_remove", array, value);
  }
}
