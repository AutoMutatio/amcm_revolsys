package com.revolsys.record.query;

import com.revolsys.record.schema.FieldDefinition;

public abstract class AbstractArrayExpression extends AbstractUnaryQueryValue {

  public AbstractArrayExpression() {
  }

  public AbstractArrayExpression(final QueryValue value) {
    super(value);
  }

  @Override
  public void setFieldDefinition(final FieldDefinition fieldDefinition) {
    getValue().setFieldDefinition(fieldDefinition);
  }
}
