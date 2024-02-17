package com.revolsys.jdbc.field;

import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.revolsys.data.type.DataTypes;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public class JdbcStringFieldDefinition extends JdbcFieldDefinition {
  private final boolean intern;

  public JdbcStringFieldDefinition(final String dbName, final String name, final int sqlType,
    final String dbDataType, final int length, final boolean required, final String description,
    final Map<String, Object> properties) {
    super(dbName, name, DataTypes.STRING, sqlType, dbDataType, length, 0, required, description,
      properties);
    this.intern = Property.getBoolean(properties, "stringIntern");
  }

  @Override
  public JdbcStringFieldDefinition clone() {
    final JdbcStringFieldDefinition clone = new JdbcStringFieldDefinition(getDbName(), getName(),
      getSqlType(), getDbDataType(), getLength(), isRequired(), getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    String value = resultSet.getString(indexes.incrementAndGet());
    if (value != null && (this.intern || internStrings)) {
      value = value.intern();
    }
    return value;
  }

  @Override
  public int setPreparedStatementArray(final PreparedStatement statement, final int parameterIndex,
    final List<?> values) throws SQLException {
    if (values == null) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      final String[] array = new String[values.size()];
      int i = 0;
      for (final Object value : values) {
        array[i++] = DataTypes.STRING.toObject(value);
      }
      final Array sqlArray = getRecordStore().newArray(statement.getConnection(), getDbDataType(),
        array);
      statement.setArray(parameterIndex, sqlArray);
    }
    return parameterIndex + 1;
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      final String string = value.toString();
      statement.setString(parameterIndex, string);
    }
    return parameterIndex + 1;
  }

}
