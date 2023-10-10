package com.revolsys.record.query;

public abstract class AbstractArrayExpression extends AbstractUnaryQueryValue {

  public AbstractArrayExpression() {
  }

  public AbstractArrayExpression(final QueryValue value) {
    super(value);
  }

  @Override
  public void setColumn(final ColumnReference column) {
    getValue().setColumn(column);
  }
}
