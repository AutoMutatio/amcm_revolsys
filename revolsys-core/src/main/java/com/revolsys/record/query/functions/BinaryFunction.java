package com.revolsys.record.query.functions;

import java.util.List;

import com.revolsys.data.type.DataType;
import com.revolsys.record.query.AbstractBinaryQueryValue;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordStore;

public abstract class BinaryFunction extends AbstractBinaryQueryValue implements Function {

  private final String name;

  public BinaryFunction(final String name, final List<QueryValue> parameters) {
    super(parameters);
    this.name = name;
  }

  public BinaryFunction(final String name, final QueryValue left, final QueryValue right) {
    super(left, right);
    this.name = name;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append(getName());
    buffer.append("(");
    appendLeft(statement, recordStore, buffer);
    buffer.append(", ");
    appendRight(statement, recordStore, buffer);
    buffer.append(")");
  }

  @Override
  public BinaryFunction clone() {
    return (BinaryFunction)super.clone();
  }

  @Override
  public boolean equals(final Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof BinaryFunction) {
      final BinaryFunction function = (BinaryFunction)other;
      if (DataType.equal(function.getName(), getName())) {
        return super.equals(function);
      }
    }
    return false;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public int getParameterCount() {
    return 2;
  }

  @Override
  public List<QueryValue> getParameters() {
    return getQueryValues();
  }

  @Override
  public String toString() {
    return getName() + "(" + getLeft().toString() + "," + getRight().toString() + ")";
  }
}
