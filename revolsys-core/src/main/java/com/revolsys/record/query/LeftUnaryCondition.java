package com.revolsys.record.query;

import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordStore;

public class LeftUnaryCondition extends AbstractUnaryQueryValue implements Condition {

  private final String operator;

  public LeftUnaryCondition(final String operator, final QueryValue value) {
    super(value);
    this.operator = operator;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append(this.operator);
    buffer.append(" ");
    super.appendDefaultSql(statement, recordStore, buffer);
  }

  @Override
  public LeftUnaryCondition clone() {
    return (LeftUnaryCondition)super.clone();
  }

  @Override
  public LeftUnaryCondition clone(final TableReference oldTable, final TableReference newTable) {
    return (LeftUnaryCondition)super.clone(oldTable, newTable);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof LeftUnaryCondition) {
      final LeftUnaryCondition condition = (LeftUnaryCondition)obj;
      if (DataType.equal(condition.getOperator(), getOperator())) {
        return super.equals(condition);
      }
    }
    return false;
  }

  public Condition getCondition() {
    return getValue();
  }

  public String getOperator() {
    return this.operator;
  }

  public void setCondition(final Condition condition) {
    setValue(condition);
  }

  @Override
  public String toString() {
    return this.operator + " " + super.toString();
  }
}
