package com.revolsys.record.query;

import java.util.function.BiFunction;

import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;

public interface TableReferenceProxy {
  default void appendColumnPrefix(final SqlAppendable string) {
    final String alias = getTableAlias();
    if (alias != null) {
      string.append('"');
      string.append(alias);
      string.append('"');
      string.append('.');
    }
  }

  default DeleteStatement deleteStatement() {
    return new DeleteStatement().from(getTableReference());
  }

  default ColumnReference getColumn(final CharSequence name) {
    return getTableReference().getColumn(name);
  }

  default FieldDefinition getField(final CharSequence name) {
    final ColumnReference column = getColumn(name);
    if (column instanceof FieldDefinition) {
      return (FieldDefinition)column;
    }
    return null;
  }

  default <R extends RecordStore> R getRecordStore() {
    return getTableReference().getRecordDefinition()
      .getRecordStore();
  }

  default String getTableAlias() {
    final TableReference table = getTableReference();
    if (table == null) {
      return null;
    } else {
      return table.getTableAlias();
    }
  }

  TableReference getTableReference();

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
      } else {
        if (left instanceof ColumnReference) {
          right = new Value((ColumnReference)left, value);
        } else {
          right = Value.newValue(value);
        }
      }
      condition = operator.apply(left, right);
    }
    return condition;
  }
}
