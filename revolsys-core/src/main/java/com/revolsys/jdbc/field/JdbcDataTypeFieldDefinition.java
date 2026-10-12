package com.revolsys.jdbc.field;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class JdbcDataTypeFieldDefinition extends JdbcFieldDefinition {
  private final JdbcDataType jdbcDataType;

  public JdbcDataTypeFieldDefinition(final String dbColumnName, final String name,
    final int sqlType, final String dbDataType, final JdbcDataType jdbcDataType, final int length,
    final int scale, final boolean required, final String description,
    final Map<String, Object> properties) {
    super(
      dbColumnName,
        name,
        jdbcDataType.dataType(),
        sqlType,
        dbDataType,
        length,
        scale,
        required,
        description,
        properties);
    this.jdbcDataType = jdbcDataType;
  }

  @Override
  public JdbcDataTypeFieldDefinition clone() {
    final JdbcDataTypeFieldDefinition clone = new JdbcDataTypeFieldDefinition(getDbName(),
      getName(), getSqlType(), getDbDataType(), this.jdbcDataType, getLength(), getScale(),
      isRequired(), getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, boolean internStrings)
    throws SQLException {
    internStrings |= getProperties().isTrue("stringIntern");
    return this.jdbcDataType.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings);
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    return this.jdbcDataType.setPreparedStatementValue(statement, parameterIndex, value);
  }
}
