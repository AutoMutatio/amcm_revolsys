package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;

public interface ColumnReference extends QueryValue {

  void appendColumnName(final SqlAppendable string);

  default void appendColumnNameWithPrefix(final SqlAppendable string) {
    appendColumnPrefix(string);
    appendColumnName(string);
  }

  default void appendColumnPrefix(final SqlAppendable string) {
  }

  @Override
  default int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  ColumnReference clone();

  @Override
  ColumnReference clone(TableReference oldTable, TableReference newTable);

  default String getAliasName() {
    return getName();
  }

  default DataType getDataType() {
    return getFieldDefinition().getDataType();
  }

  FieldDefinition getFieldDefinition();

  String getName();

  @Override
  String getStringValue(final MapEx record);

  TableReferenceProxy getTable();

  @Override
  @SuppressWarnings("unchecked")
  default <V> V getValue(final MapEx record) {
    if (record == null) {
      return null;
    } else {
      final CharSequence name = getName();
      final DataType dataType = getDataType();
      return (V)record.getTypedValue(name, dataType);
    }
  }

  default Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings,
    final String aliasName) throws SQLException {
    return getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
  }

  @SuppressWarnings("unchecked")
  default <V> V toColumnType(final Object value) {
    try {
      return toColumnTypeException(value);
    } catch (final Throwable e) {
      return (V)value;
    }
  }

  <V> V toColumnTypeException(final Object value);

  @SuppressWarnings("unchecked")
  default <V> V toFieldValue(final Object value) {
    try {
      return toColumnTypeException(value);
    } catch (final Throwable e) {
      return (V)value;
    }
  }

  <V> V toFieldValueException(final Object value);

  <V> V toFieldValueException(RecordState state, Object value);

  String toString(Object value);
}
