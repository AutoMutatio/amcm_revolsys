package com.revolsys.record.query;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.TypedIdentifier;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.date.Dates;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinitions;
import com.revolsys.record.code.CodeTable;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public class Value implements QueryValue {
  private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter
    .ofPattern("yyyy-MM-dd'T'HH:mm:ss.S")
    .withZone(ZoneId.systemDefault());

  public static boolean isString(final QueryValue queryValue) {
    if (queryValue instanceof Value) {
      final Value value = (Value)queryValue;
      return value.getValue() instanceof String;

    }
    return false;
  }

  public static Value newValue(final ColumnReference field, final Object value) {
    if (field == null) {
      return newValue(value);
    } else {
      return new Value(field, value);
    }
  }

  public static Value newValue(final FieldDefinition field, final Object value) {
    if (field == null) {
      return newValue(value);
    } else {
      return new Value(field, value);
    }
  }

  public static Value newValue(final FieldDefinition field, final Object value,
    final boolean dontConvert) {
    if (field == null) {
      return newValue(value);
    } else {
      return new Value(field, value, dontConvert);
    }
  }

  public static Value newValue(final Object value) {
    return newValue(JdbcFieldDefinitions.newFieldDefinition(value), value);
  }

  public static Value newValue(final QueryValue fieldValue, final Object value) {
    ColumnReference columnRef;
    if (fieldValue instanceof final ColumnReference col) {
      columnRef = col;
    } else {
      columnRef = JdbcFieldDefinitions.newFieldDefinition(value);
    }
    return newValue(columnRef, value);
  }

  public static Value newValue(final RecordDefinitionProxy table, final String fieldName,
    final Object value) {
    final var field = table.getColumn(fieldName);
    return newValue(field, value);
  }

  public static String toString(final Object displayValue) {
    if (displayValue == null) {
      return "null";
    } else if (displayValue instanceof final Number number) {
      return DataTypes.toString(number);
    } else if (displayValue instanceof final Date date) {
      final String stringValue = Dates.format("yyyy-MM-dd", date);
      return "{d '" + stringValue + "'}";
    } else if (displayValue instanceof final Time time) {
      final String stringValue = Dates.format("HH:mm:ss", time);
      return "{t '" + stringValue + "'}";
    } else if (displayValue instanceof final Instant instant) {
      return "{i '" + TIMESTAMP_FORMAT.format(instant) + "'}";
    } else if (displayValue instanceof final Timestamp time) {
      final String stringValue = TIMESTAMP_FORMAT.format(time.toInstant());
      return "{ts '" + stringValue + "'}";
    } else if (displayValue instanceof final java.util.Date time) {
      final String stringValue = TIMESTAMP_FORMAT.format(time.toInstant());
      return "{ts '" + stringValue + "'}";
    } else {
      final Object value = displayValue;
      final String string = DataTypes.toString(value);
      return "'" + string.replaceAll("'", "''") + "'";
    }
  }

  public static Object toValue(final Object value) {
    if (value instanceof TypedIdentifier) {
      final Identifier identifier = (Identifier)value;
      return identifier;
    } else if (value instanceof Identifier) {
      final Identifier identifier = (Identifier)value;
      return identifier.toSingleValue();
    } else {
      return value;
    }
  }

  private ColumnReference column;

  private Object displayValue;

  private JdbcFieldDefinition jdbcField;

  private Object queryValue;

  private boolean dontConvert;

  public Value(final ColumnReference column, Object value) {
    this.column = column;
    if (column instanceof final JdbcFieldDefinition jdbcField) {
      this.jdbcField = jdbcField;
    }
    value = toValue(value);
    this.displayValue = column.toColumnType(value);
    this.queryValue = column.toFieldValue(this.displayValue);
  }

  protected Value(final FieldDefinition field, final Object value) {
    this.column = field;
    if (this.column instanceof final JdbcFieldDefinition jdbcField) {
      this.jdbcField = jdbcField;
    }
    setQueryValue(value);
    this.displayValue = this.queryValue;
    setColumn(field);
  }

  public Value(final FieldDefinition field, final Object value, final boolean dontConvert) {
    this.column = field;
    if (this.column instanceof final JdbcFieldDefinition jdbcField) {
      this.jdbcField = jdbcField;
    }
    this.dontConvert = dontConvert;
    if (dontConvert) {
      this.queryValue = toValue(value);
      this.displayValue = this.queryValue;
      if (field != null) {
        this.column = field;
        if (field instanceof JdbcFieldDefinition) {
          this.jdbcField = (JdbcFieldDefinition)field;
        } else {
          this.jdbcField = JdbcFieldDefinitions.newFieldDefinition(this.queryValue);
        }
      }
    } else {
      setQueryValue(value);
      this.displayValue = this.queryValue;
      setColumn(field);
    }
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (sql.isUsePlaceholders()) {
      if (this.jdbcField == null) {
        sql.append('?');
      } else {
        this.jdbcField.addSelectStatementPlaceHolder(sql);
      }
    } else {
      if (this.jdbcField == null) {
        if (recordStore == null) {
          RecordStore.appendDefaultSql(sql, this.queryValue);
        } else {
          recordStore.appendSqlValue(sql, this.queryValue);
        }
      } else {
        this.jdbcField.appendSqlValue(sql, recordStore, this.queryValue);
      }
    }
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    try {
      try {
        return this.jdbcField.setPreparedStatementValue(statement, index, this.queryValue);
      } catch (final IllegalArgumentException e) {
        return this.jdbcField.setPreparedStatementValue(statement, index, null);
      }
    } catch (final SQLException e) {
      throw Exceptions.wrap("Unable to set value", e)
        .property("value", this.queryValue);
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
  public Value clone() {
    try {
      return (Value)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public Value clone(final TableReference oldTable, final TableReference newTable) {
    final Value clone = clone();
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

  public void convert(final DataType dataType) {
    if (this.queryValue != null) {
      final Object value = this.queryValue;
      final Object newValue = dataType.toObject(value);
      final Class<?> typeClass = dataType.getJavaClass();
      if (newValue == null || !typeClass.isAssignableFrom(newValue.getClass())) {
        throw new IllegalArgumentException(
          "'" + this.queryValue + "' is not a valid " + dataType.getValidationName());
      } else {
        setQueryValue(newValue);
      }
    }
  }

  public void convert(final FieldDefinition field) {
    if (field instanceof JdbcFieldDefinition) {
      this.jdbcField = (JdbcFieldDefinition)field;
    }
    convert(field.getDataType());
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Value) {
      final Value value = (Value)obj;
      return DataType.equal(value.getValue(), this.getValue());
    } else {
      return false;
    }
  }

  public Object getDisplayValue() {
    return this.displayValue;
  }

  public JdbcFieldDefinition getJdbcField() {
    return this.jdbcField;
  }

  public Object getQueryValue() {
    return this.queryValue;
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
    return this.queryValue;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {
    return (V)this.queryValue;
  }

  @Override
  public void setColumn(final ColumnReference column) {
    if (column != null) {
      this.column = column;
      if (column != null) {
        final FieldDefinition field = column.getFieldDefinition();
        if (field instanceof JdbcFieldDefinition) {
          this.jdbcField = (JdbcFieldDefinition)field;
        } else {
          this.jdbcField = JdbcFieldDefinitions.newFieldDefinition(this.queryValue);
        }
        if (!this.dontConvert) {
          this.queryValue = column.toFieldValue(this.queryValue);
        }
        CodeTable codeTable = null;
        final TableReferenceProxy table = column.getTable();
        if (table != null) {
          final TableReference tableRef = table.getTableReference();
          if (tableRef instanceof final RecordDefinition recordDefinition) {
            final String fieldName = column.getName();
            codeTable = recordDefinition.getCodeTableByFieldName(fieldName);
            if (codeTable instanceof RecordDefinitionProxy) {
              final RecordDefinitionProxy proxy = (RecordDefinitionProxy)codeTable;
              if (proxy.getRecordDefinition() == recordDefinition) {
                codeTable = null;
              }
            }
            if (codeTable != null) {
              final Identifier id = codeTable.getIdentifier(this.queryValue);
              if (id == null) {
                this.displayValue = this.queryValue;
              } else {
                setQueryValue(id);
                final List<Object> values = codeTable.getValues(id);
                if (values.size() == 1) {
                  this.displayValue = values.get(0);
                } else {
                  this.displayValue = Strings.toString(":", values);
                }
              }
            }
          }
        }
      }
    }
  }

  public Value setQueryValue(final Object value) {
    this.queryValue = toValue(value);
    return this;
  }

  public void setValue(Object value) {
    value = toValue(value);
    if (this.column.getName() == JdbcFieldDefinitions.UNKNOWN) {
      this.column = JdbcFieldDefinitions.newFieldDefinition(value);
    }
    setQueryValue(value);
  }

  @Override
  public String toFormattedString() {
    return toString();
  }

  @Override
  public String toString() {
    final Object displayValue = this.displayValue;
    return toString(displayValue);
  }
}
