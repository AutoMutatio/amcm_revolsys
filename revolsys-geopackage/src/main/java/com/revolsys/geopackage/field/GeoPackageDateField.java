package com.revolsys.geopackage.field;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.data.type.DataTypes;
import com.revolsys.date.Dates;
import com.revolsys.jdbc.field.JdbcDateFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.util.Property;

public class GeoPackageDateField extends JdbcFieldDefinition {
  public GeoPackageDateField(final String dbName, final String name, final int sqlType,
    final String dbDataType, final boolean required, final String description,
    final Map<String, Object> properties) {
    super(dbName, name, DataTypes.SQL_DATE, sqlType, dbDataType, 0, 0, required, description,
      properties);
  }

  @Override
  public JdbcDateFieldDefinition clone() {
    return new JdbcDateFieldDefinition(getDbName(), getName(), getSqlType(), getDbDataType(),
      isRequired(), getDescription(), getProperties());
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final String dateString = resultSet.getString(indexes.incrementAndGet());
    return Dates.getSqlDate(dateString);
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (Property.isEmpty(value)) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      final Date date = Dates.getSqlDate(value);
      final String dateString = Dates.toSqlDateString(date);
      statement.setString(parameterIndex, dateString);
    }
    return parameterIndex + 1;
  }
}
