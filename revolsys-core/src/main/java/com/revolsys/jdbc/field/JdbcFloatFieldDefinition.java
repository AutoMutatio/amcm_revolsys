package com.revolsys.jdbc.field;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.data.type.DataTypes;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class JdbcFloatFieldDefinition extends JdbcFieldDefinition {
  public JdbcFloatFieldDefinition(final String dbName, final String name, final int sqlType,
    final String dbDataType, final boolean required, final String description,
    final Map<String, Object> properties) {
    super(dbName, name, DataTypes.FLOAT, sqlType, dbDataType, 11, 0, required, description,
      properties);
  }

  @Override
  public JdbcFloatFieldDefinition clone() {
    final JdbcFloatFieldDefinition clone = new JdbcFloatFieldDefinition(getDbName(), getName(),
      getSqlType(), getDbDataType(), isRequired(), getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final float value = resultSet.getFloat(indexes.incrementAndGet());
    if (resultSet.wasNull()) {
      return null;
    } else {
      return Float.valueOf(value);
    }
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      statement.setNull(parameterIndex, getSqlType());
    } else {
      float numberValue;
      if (value instanceof Number) {
        final Number number = (Number)value;
        numberValue = number.floatValue();
      } else {
        numberValue = Float.parseFloat(value.toString());
      }
      statement.setFloat(parameterIndex, numberValue);
    }
    return parameterIndex + 1;
  }
}
