package com.revolsys.record.query;

import java.util.function.BiFunction;

public interface QueryValueFactory {

  ColumnReference getColumn(CharSequence name);

  default Condition newCondition(final CharSequence fieldName,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    final ColumnReference left = getColumn(fieldName);
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else {
        right = new Value(left, value);
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  default Condition newCondition(final CharSequence fieldName,
    final java.util.function.Function<QueryValue, Condition> operator) {
    final ColumnReference column = getColumn(fieldName);
    final Condition condition = operator.apply(column);
    return condition;
  }

  default Condition newCondition(final QueryValue left,
    final BiFunction<QueryValue, QueryValue, Condition> operator, final Object value) {
    Condition condition;
    if (value == null) {
      condition = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof QueryValue) {
        right = (QueryValue)value;
      } else if (left instanceof ColumnReference) {
        right = new Value((ColumnReference)left, value);
      } else {
        right = Value.newValue(value);
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }

  default <V extends QueryValue> V queryValue(final CharSequence fieldName,
    final BiFunction<QueryValue, QueryValue, V> operator, final Object value) {
    final ColumnReference left = getColumn(fieldName);
    V condition;
    QueryValue right;
    if (value instanceof QueryValue) {
      right = (QueryValue)value;
    } else {
      right = new Value(left, value);
    }
    condition = operator.apply(left, right);
    return condition;
  }
}
