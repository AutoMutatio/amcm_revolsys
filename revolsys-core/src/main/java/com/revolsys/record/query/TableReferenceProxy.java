package com.revolsys.record.query;

import java.util.function.BiFunction;

import com.revolsys.collection.json.Json;
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

  default <V> V column(final CharSequence fieldName,
    final java.util.function.Function<QueryValue, V> operator) {
    final ColumnReference column = getColumn(fieldName);
    return operator.apply(column);
  }

  default QueryValue columnByPath(final String path) {
    final var parts = path.split("\\.");
    final var fieldName = parts[0];
    final var column = getColumn(fieldName);
    if (column == null) {
      return new UnknownColumn(fieldName);
    }
    QueryValue result = column;
    if (parts.length - 0 > 1) {
      if (column.getDataType() == Json.JSON_OBJECT || column.getDataType() == Json.JSON_TYPE) {
        for (int i = 0 + 1; i < parts.length; i++) {
          final var part = parts[i];
          result = Q.jsonRawValue(result, part);
        }
      } else {
        throw new IllegalStateException("Field path can only be specified for json fields");
      }
    }
    return result;
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
    final var tableReference = getTableReference();
    if (tableReference == null) {
      return null;
    }
    return tableReference.getRecordDefinition()
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
