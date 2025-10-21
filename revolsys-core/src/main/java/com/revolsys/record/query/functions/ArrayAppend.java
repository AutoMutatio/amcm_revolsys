package com.revolsys.record.query.functions;

import com.revolsys.record.query.QueryValue;

public class ArrayAppend extends SimpleFunction {

  public ArrayAppend(final QueryValue array, final QueryValue value) {
    super("array_append", array, value);
  }
}
