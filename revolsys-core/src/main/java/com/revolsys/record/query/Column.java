package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.RecordState;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class Column implements QueryValue, ColumnReference {

  private final String name;

  private String alias;

  private TableReferenceProxy table;

  public Column(final CharSequence name) {
    this(name.toString());
  }

  public Column(final String name) {
    this.name = name;
  }

  public Column(final TableReferenceProxy tableReference, final CharSequence name) {
    this.table = tableReference;
    this.name = name.toString();
  }

  @Override
  public void appendColumnName(final SqlAppendable string) {
    final FieldDefinition fieldDefinition = getFieldDefinition();
    if (fieldDefinition == null) {
      final String name = this.name;
      if ("*".equals(name) || name.indexOf('"') != -1 || name.indexOf('.') != -1
        || name.matches("([A-Z][_A-Z1-9]*\\.)?[A-Z][_A-Z1-9]*\\*")) {
        string.append(name);
      } else {
        string.append('"');
        string.append(name);
        string.append('"');
      }
    } else {
      fieldDefinition.appendColumnName(string);
    }
  }

  @Override
  public void appendColumnPrefix(final SqlAppendable string) {
    if (this.table != null) {
      this.table.appendColumnPrefix(string);
    }
  }

  @Override
  public void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendColumnNameWithPrefix(sql);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendColumnNameWithPrefix(sql);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  @Override
  public Column clone() {
    try {
      return (Column)super.clone();
    } catch (final CloneNotSupportedException e) {
      return null;
    }
  }

  @Override
  public ColumnReference clone(final TableReference oldTable, final TableReference newTable) {
    if (oldTable != newTable && !(this.table instanceof Join)) {
      final ColumnReference newColumn = newTable.getColumn(this.name);
      if (newColumn != null) {
        return newColumn;
      }
    }
    return clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Column) {
      final ColumnReference value = (ColumnReference)obj;
      return DataType.equal(value.getName(), getName());
    } else {
      return false;
    }
  }

  @Override
  public FieldDefinition getFieldDefinition() {
    if (this.table != null) {
      return this.table.getField(this.name);
    }
    return null;
  }

  @Override
  public int getFieldIndex() {
    final FieldDefinition fieldDefinition = getFieldDefinition();
    if (fieldDefinition == null) {
      return -1;
    } else {
      return fieldDefinition.getIndex();
    }
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public String getStringValue(final MapEx record) {
    final Object value = getValue(record);
    final FieldDefinition fieldDefinition = getFieldDefinition();
    if (fieldDefinition == null) {
      return DataTypes.toString(value);
    } else {
      return fieldDefinition.toString(value);
    }
  }

  @Override
  public TableReferenceProxy getTable() {
    return this.table;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <V> V getValue(final MapEx record) {
    if (record == null) {
      return null;
    } else {
      final String name = getName();
      if (name.indexOf('.') == -1) {
        return (V)record.getValue(name);
      } else {
        return (V)record.getValueByPath(name);
      }
    }
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    var name = this.name;
    if (this.alias != null) {
      name = this.alias;
    }
    final FieldDefinition field = recordDefinition.getField(name);
    if (field == null) {
      return recordDefinition.getField(fieldIndex)
        .getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes, internStrings);
    } else {
      return field.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
        internStrings);
    }
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings,
    final String alias) throws SQLException {
    var name = this.name;
    if (this.alias != null) {
      name = this.alias;
    }
    FieldDefinition field = recordDefinition.getField(name);
    if (field == null) {
      field = recordDefinition.getField(alias);
    }
    return field.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings);
  }

  @Override
  public int hashCode() {
    return this.name.hashCode();
  }

  @Override
  public QueryValue toAlias(final String alias) {
    this.alias = alias;
    return ColumnReference.super.toAlias(alias);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V toColumnTypeException(final Object value) {
    if (value == null) {
      return null;
    } else {
      final FieldDefinition fieldDefinition = getFieldDefinition();
      if (fieldDefinition == null) {
        return (V)value;
      } else {
        return fieldDefinition.toColumnTypeException(value);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V toFieldValueException(final Object value) {
    if (value == null) {
      return null;
    } else {
      final FieldDefinition fieldDefinition = getFieldDefinition();
      if (fieldDefinition == null) {
        return (V)value;
      } else {
        return fieldDefinition.toFieldValueException(value);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V toFieldValueException(final RecordState state, final Object value) {
    if (value == null) {
      return null;
    } else {
      final FieldDefinition fieldDefinition = getFieldDefinition();
      if (fieldDefinition == null) {
        return (V)value;
      } else {
        return fieldDefinition.toFieldValueException(state, value);
      }
    }
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    appendColumnNameWithPrefix(sql);
    return sql.toString();
  }

  @Override
  public String toString(final Object value) {
    final FieldDefinition fieldDefinition = getFieldDefinition();
    if (fieldDefinition == null) {
      return DataTypes.toString(value);
    } else {
      return fieldDefinition.toString(value);
    }
  }
}
