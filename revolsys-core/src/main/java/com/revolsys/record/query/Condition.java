package com.revolsys.record.query;

import java.util.function.Predicate;

import com.revolsys.collection.map.MapEx;
import com.revolsys.util.Emptyable;

public interface Condition extends QueryValue, Predicate<MapEx>, Emptyable {

  AcceptAllCondition ALL = new AcceptAllCondition();

  default Condition and(final Condition condition) {
    if (condition == null || com.revolsys.util.Property.isEmpty(condition)) {
      return this;
    } else if (com.revolsys.util.Property.isEmpty(this)) {
      return condition;
    } else {
      return new And(this, condition);
    }
  }

  Condition clone();

  @Override
  Condition clone(TableReference oldTable, TableReference newTable);

  @SuppressWarnings("unchecked")
  @Override
  default <V> V getValue(final MapEx record) {
    final Boolean value = test(record);
    return (V)value;
  }

  @Override
  default boolean isEmpty() {
    return false;
  }

  default Condition not() {
    if (com.revolsys.util.Property.isEmpty(this)) {
      return this;
    } else {
      return new Not(this);
    }
  }

  default Condition or(final Condition condition) {
    if (condition == null || com.revolsys.util.Property.isEmpty(condition)) {
      return this;
    } else if (com.revolsys.util.Property.isEmpty(this)) {
      return condition;
    } else {
      return new Or(this, condition);
    }
  }

  @Override
  default boolean test(final MapEx record) {
    throw new UnsupportedOperationException("Cannot filter using " + toString());
  }

  @Override
  default String toFormattedString() {
    return toString();
  }

}
