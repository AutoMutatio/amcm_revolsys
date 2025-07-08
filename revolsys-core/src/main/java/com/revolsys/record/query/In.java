package com.revolsys.record.query;

import java.util.Arrays;
import java.util.Collection;

import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.schema.RecordStore;

public class In extends AbstractBinaryQueryValue implements Condition {
  public static In create(final QueryValue left, final Collection<? extends Object> conditions) {
    final var column = left.getColumn();
    final var right = new CollectionValue(column, conditions);
    return create(left, right);
  }

  public static In create(final QueryValue left, QueryValue right) {
    final var column = left.getColumn();
    if (right instanceof Value) {
      right = new CollectionValue(column, Arrays.asList(((Value)right).getValue()));
    } else {
      if (column != null && right instanceof CollectionValue) {
        right.setColumn(column);
      }
    }
    return new In(left, right);
  }

  private In(final QueryValue left, final QueryValue right) {
    super(left, right);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    if (isEmpty()) {
      buffer.append("1==0");
    } else {
      super.appendLeft(statement, recordStore, buffer);
      buffer.append(" IN ");
      final boolean collection = getRight() instanceof CollectionValue;
      if (!collection) {
        buffer.append('(');
      }
      super.appendRight(statement, recordStore, buffer);
      if (!collection) {
        buffer.append(')');
      }
    }
  }

  @Override
  public In clone() {
    final In clone = (In)super.clone();
    return clone;
  }

  @Override
  public In clone(final TableReference oldTable, final TableReference newTable) {
    return (In)super.clone(oldTable, newTable);
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof In) {
      final In in = (In)obj;
      return super.equals(in);
    }
    return false;
  }

  public CollectionValue getValues() {
    return (CollectionValue)getRight();
  }

  @Override
  public boolean isEmpty() {
    final QueryValue right = getRight();
    if (right instanceof final CollectionValue collection) {
      return collection.isEmpty();
    } else {
      return false;
    }
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getLeft();
    final QueryValue right = getRight();

    final var list1 = toCollection(left, record);
    final var list2 = toCollection(right, record);

    if (list1.isEmpty() || list2.isEmpty()) {
      return false;
    }
    for (final Object v1 : list1) {
      if (list2.contains(v1)) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  private Collection<Object> toCollection(final QueryValue queryValue, final MapEx record) {
    if (queryValue instanceof final CollectionValue collectionValue) {
      return collectionValue.getValues();
    }
    final var value = queryValue.getValue(record);
    if (value == null) {
      return Lists.empty();
    } else if (value instanceof final Collection collection) {
      return collection;
    } else {
      return Lists.newArray(value);
    }
  }

  @Override
  public String toString() {
    final Object value = getLeft();
    final Object value1 = getRight();
    return DataTypes.toString(value) + " IN " + DataTypes.toString(value1);
  }
}
