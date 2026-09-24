package com.revolsys.jdbc.field;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.data.type.DataTypes;
import com.revolsys.date.Dates;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public class JdbcDateFieldDefinition extends JdbcFieldDefinition {
  public JdbcDateFieldDefinition(final String dbName, final String name, final int sqlType,
    final String dbDataType, final boolean required, final String description,
    final Map<String, Object> properties) {
    super(dbName, name, DataTypes.SQL_DATE, sqlType, dbDataType, 0, 0, required, description,
      properties);
  }

  @Override
  public JdbcDateFieldDefinition clone() {
    final JdbcDateFieldDefinition clone = new JdbcDateFieldDefinition(getDbName(), getName(),
      getSqlType(), getDbDataType(), isRequired(), getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return resultSet.getDate(indexes.incrementAndGet());
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (Property.isEmpty(value)) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      final Date date = Dates.getSqlDate(value);
      statement.setDate(parameterIndex, date);
    }
    return parameterIndex + 1;
  }
}
