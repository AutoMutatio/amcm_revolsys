package com.revolsys.record.query;

import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.schema.RecordStore;

public abstract class UnaryArithmatic extends AbstractBinaryQueryValue {

  private final String operator;

  public UnaryArithmatic(final QueryValue left, final String operator, final QueryValue right) {
    super(left, right);
    this.operator = operator;
  }

  public UnaryArithmatic(final String name, final String operator, final Object value) {
    this(new Column(name), operator, Value.newValue(value));
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    appendLeft(statement, recordStore, buffer);
    buffer.append(this.operator);
    appendRight(statement, recordStore, buffer);
  }

  @Override
  public UnaryArithmatic clone() {
    return (UnaryArithmatic)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof UnaryArithmatic) {
      final UnaryArithmatic condition = (UnaryArithmatic)obj;
      if (DataType.equal(condition.getOperator(), getOperator())) {
        return super.equals(condition);
      }
    }
    return false;
  }

  public String getOperator() {
    return this.operator;
  }

  @Override
  public String toString() {
    final Object left = getLeft();
    final Object right = getRight();
    return DataTypes.toString(left) + " " + this.operator + " " + DataTypes.toString(right);
  }

}
