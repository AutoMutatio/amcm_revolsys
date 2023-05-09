package com.revolsys.record.query.functions;

import com.revolsys.record.query.Condition;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.TableReference;

public class Exists extends UnaryFunction implements Condition {

  public Exists(final QueryValue expression) {
    super("EXISTS", expression);
  }

  @Override
  public Exists clone() {
    return (Exists)super.clone();
  }

  @Override
  public Exists clone(final TableReference oldTable, final TableReference newTable) {
    return (Exists)super.clone(oldTable, newTable);
  }
}
