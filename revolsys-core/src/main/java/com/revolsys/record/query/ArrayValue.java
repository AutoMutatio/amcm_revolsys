package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinitions;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Property;

public class ArrayValue implements QueryValue {

  private ColumnReference column;

  private JdbcFieldDefinition jdbcField;

  private ListEx<Object> values = new ArrayListEx<>();

  private boolean dontConvert;

  public ArrayValue(final ColumnReference column) {
    setColumn(column);
  }

  public ArrayValue(final ColumnReference column, final Collection<Object> values) {
    this.values.addAll(values);
    setColumn(column);
  }

  public void addValue(Object value) {
    if (this.jdbcField != null) {
      value = this.jdbcField.toFieldValue(value);
    }
    this.values.add(value);
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (sql.isUsePlaceholders()) {
      sql.append('?');
    } else {
      sql.append("ARRAY[");
      boolean first = true;
      for (final Object object : this.values) {
        if (first) {
          first = false;
        } else {
          sql.append(',');
        }
        final Value value = Value.newValue(this.jdbcField, object);
        value.appendDefaultSelect(query, recordStore, sql);
      }
      sql.append(']');
    }
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    try {
      return this.jdbcField.setPreparedStatementArray(statement, index, this.values);
    } catch (final SQLException e) {
      throw new RuntimeException("Unable to set value: " + this.values, e);
    }
  }

  @Override
  public void changeRecordDefinition(final RecordDefinition oldRecordDefinition,
    final RecordDefinition newRecordDefinition) {
    final String fieldName = this.column.getName();
    if (Property.hasValue(fieldName)) {
      final FieldDefinition column = newRecordDefinition.getField(fieldName);
      setColumn(column);
    }
  }

  @Override
  public ArrayValue clone() {
    try {
      return (ArrayValue)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public ArrayValue clone(final TableReference oldTable, final TableReference newTable) {
    final ArrayValue clone = clone();
    if (oldTable != newTable && this.column.getTable() == oldTable) {
      final String name = this.column.getName();
      if (name != JdbcFieldDefinitions.UNKNOWN) {
        final ColumnReference newColumn = newTable.getColumn(name);
        if (newColumn != null) {
          setColumn(newColumn);
        }
      }
    }
    return clone;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof final ArrayValue value) {
      return DataType.equal(value.getValue(), getValue());
    } else {
      return false;
    }
  }

  public JdbcFieldDefinition getJdbcField() {
    return this.jdbcField;
  }

  public Object getQueryValue() {
    return this.values;
  }

  @Override
  public String getStringValue(final MapEx record) {
    final Object value = getValue(record);
    if (this.column == null) {
      return DataTypes.toString(value);
    } else {
      return this.column.toString(value);
    }
  }

  public Object getValue() {
    return this.values;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {
    return (V)this.values;
  }

  @Override
  public void setColumn(final ColumnReference column) {
    this.column = column;
    if (column != null) {
      if (column instanceof JdbcFieldDefinition) {
        this.jdbcField = (JdbcFieldDefinition)column;
      } else if (this.values.size() > 0) {
        this.jdbcField = JdbcFieldDefinitions.newFieldDefinition(this.values.get(0));
      }
      if (!this.dontConvert) {
        this.values = this.values.map(this.jdbcField::toFieldValue).toList();
      }
    }
  }

  @Override
  public String toFormattedString() {
    return toString();
  }

  @Override
  public String toString() {
    return "ARRAY[" + this.values.map(Value::toString).join(",") + ']';
  }
}
