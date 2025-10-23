package com.revolsys.gis.postgresql.type;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.revolsys.data.type.DataTypes;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class PostgreSQLJdbcEnumFieldDefinition extends JdbcFieldDefinition {

  public PostgreSQLJdbcEnumFieldDefinition(final String dbName, final String name,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description, final Map<String, Object> properties) {
    super(
      dbName,
        name,
        DataTypes.STRING,
        sqlType,
        dbDataType,
        length,
        scale,
        required,
        description,
        properties);
  }

  @Override
  public PostgreSQLJdbcEnumFieldDefinition clone() {
    final PostgreSQLJdbcEnumFieldDefinition clone = new PostgreSQLJdbcEnumFieldDefinition(
      getDbName(), getName(), getSqlType(), getDbDataType(), getLength(), getScale(), isRequired(),
      getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return resultSet.getString(indexes.incrementAndGet());
  }

  @Override
  public int setInsertPreparedStatementValue(final PreparedStatement statement,
    final int parameterIndex, final Object value) throws SQLException {
    if (value == null) {
      statement.setNull(parameterIndex, Types.OTHER, getDbDataType());
    } else {
      statement.setObject(parameterIndex, value.toString(), Types.OTHER);
    }

    return parameterIndex + 1;
  }

  @Override
  public int setPreparedStatementArray(final PreparedStatement statement, final int parameterIndex,
    final List<?> values) throws SQLException {
    return setPreparedStatementValue(statement, parameterIndex, values);
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    return setInsertPreparedStatementValue(statement, parameterIndex, value);
  }

}
