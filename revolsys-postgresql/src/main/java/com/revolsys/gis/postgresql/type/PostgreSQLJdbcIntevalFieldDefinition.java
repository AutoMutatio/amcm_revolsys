package com.revolsys.gis.postgresql.type;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;

import org.postgresql.util.PGInterval;

import com.revolsys.data.type.DataTypes;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class PostgreSQLJdbcIntevalFieldDefinition extends JdbcFieldDefinition {

  public PostgreSQLJdbcIntevalFieldDefinition(final String dbName, final String name,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description, final Map<String, Object> properties) {
    super(
      dbName,
        name,
        DataTypes.DURATION,
        sqlType,
        dbDataType,
        length,
        scale,
        required,
        description,
        properties);
  }

  @Override
  public PostgreSQLJdbcIntevalFieldDefinition clone() {
    final PostgreSQLJdbcIntevalFieldDefinition clone = new PostgreSQLJdbcIntevalFieldDefinition(
      getDbName(), getName(), getSqlType(), getDbDataType(), getLength(), getScale(), isRequired(),
      getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final Object value = resultSet.getObject(indexes.incrementAndGet());
    if (value instanceof final PGInterval interval) {
      final var start = new GregorianCalendar();
      start.setTimeInMillis(0);

      final var finish = new GregorianCalendar();
      finish.setTimeInMillis(0);
      interval.add(finish);
      final var duration = Duration.between(start.toInstant(), finish.toInstant());
      return duration;
    }
    return value;
  }

  @Override
  public int setInsertPreparedStatementValue(final PreparedStatement statement,
    final int parameterIndex, final Object value) throws SQLException {
    if (value instanceof final Duration duration) {
      final var seconds = duration.getSeconds();
      final var nanos = duration.getNano();
      final var secondFraction = seconds + nanos / 1000000000.0;
      final var interval = new PGInterval(0, 0, 0, 0, 0, secondFraction);
      statement.setObject(parameterIndex, interval);
    } else {
      throw new IllegalArgumentException("Expecting a duration not: " + value.getClass());
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
