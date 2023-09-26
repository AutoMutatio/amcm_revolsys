package com.revolsys.record.query.functions;

import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Not;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.TableReference;

public class Exists extends UnaryFunction implements Condition {

  public Exists(final QueryValue expression) {
    super("EXISTS", expression);
  }

  public Not asNot() {
    return new Not(this);
  }

  @Override
  public Exists clone() {
    return (Exists)super.clone();
  }

  @Override
  public Exists clone(final TableReference oldTable, final TableReference newTable) {
    return clone();
  }
}
