package com.revolsys.record.query;

import java.util.Arrays;
import java.util.Collection;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public class AllOperator extends AbstractBinaryQueryValue implements Condition {

  public AllOperator(final QueryValue left, final CollectionValue values) {
    super(left, values);
    if (left instanceof final ColumnReference column) {
      values.setColumn(column);
    }
  }

  public AllOperator(final QueryValue left, QueryValue right) {
    super(left, right);
    if (right instanceof Value) {
      right = new CollectionValue(Arrays.asList(((Value)right).getValue()));
      setRight(right);
    }
    if (left instanceof final ColumnReference column && right instanceof CollectionValue) {
      right.setColumn(column);
    }
  }

  public AllOperator(final String name, final Collection<? extends Object> values) {
    this(new Column(name), new CollectionValue(values));
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    throw new UnsupportedOperationException("This operator must be converted to another operator");
  }

  @Override
  public AllOperator clone() {
    return (AllOperator)super.clone();
  }

  @Override
  public AllOperator clone(final TableReference oldTable, final TableReference newTable) {
    return (AllOperator)super.clone(oldTable, newTable);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof AllOperator) {
      return super.equals(obj);
    }
    return false;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    throw new UnsupportedOperationException("This operator must be converted to another operator");
  }

  public CollectionValue getValues() {
    return (CollectionValue)getRight();
  }

  @Override
  public String toString() {
    return "ALL(" + super.toString() + ")";
  }
}
