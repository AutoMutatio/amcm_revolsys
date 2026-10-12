package com.revolsys.jdbc.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.TypedIdentifier;
import com.revolsys.date.Dates;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public interface JdbcPreparedStatementValueHandler {

  public class Handlers {

    private static final JdbcPreparedStatementValueHandler HANDLER_BOOLEAN = new LambaJdbcPreparedStatementValueHandler(
      Types.BOOLEAN, Handlers::getBoolean, Handlers::setBoolean);

    private static final JdbcPreparedStatementValueHandler HANDLER_TIMESTAMP = new LambaJdbcPreparedStatementValueHandler(
      Types.TIMESTAMP, Handlers::getTimestamp, Handlers::setTimestamp);

    private static final JdbcPreparedStatementValueHandler HANDLER_DATE = new LambaJdbcPreparedStatementValueHandler(
      Types.DATE, Handlers::getDate, Handlers::setDate);

    private static final JdbcPreparedStatementValueHandler HANDLER_BIG_DECIMAL = new LambaJdbcPreparedStatementValueHandler(
      Types.NUMERIC, Handlers::getBigDecimal, Handlers::setBigDecimal);

    private static final JdbcPreparedStatementValueHandler HANDLER_FLOAT = new LambaJdbcPreparedStatementValueHandler(
      Types.FLOAT, Handlers::getFloat, Handlers::setFloat);

    private static final JdbcPreparedStatementValueHandler HANDLER_DOUBLE = new LambaJdbcPreparedStatementValueHandler(
      Types.DOUBLE, Handlers::getDouble, Handlers::setDouble);

    private static final JdbcPreparedStatementValueHandler HANDLER_BYTE = new LambaJdbcPreparedStatementValueHandler(
      Types.TINYINT, Handlers::getByte, Handlers::setByte);

    private static final JdbcPreparedStatementValueHandler HANDLER_SHORT = new LambaJdbcPreparedStatementValueHandler(
      Types.SMALLINT, Handlers::getShort, Handlers::setShort);

    private static final JdbcPreparedStatementValueHandler HANDLER_INTEGER = new LambaJdbcPreparedStatementValueHandler(
      Types.INTEGER, Handlers::getInteger, Handlers::setInteger);

    private static final JdbcPreparedStatementValueHandler HANDLER_LONG = new LambaJdbcPreparedStatementValueHandler(
      Types.BIGINT, Handlers::getLong, Handlers::setLong);

    private static final JdbcPreparedStatementValueHandler HANDLER_STRING = new LambaJdbcPreparedStatementValueHandler(
      Types.CHAR, Handlers::getString, Handlers::setString);

    private static final JdbcPreparedStatementValueHandler HANDLER_OBJECT = new LambaJdbcPreparedStatementValueHandler(
      Types.OTHER, Handlers::getObject, Handlers::setObject);

    public static BigDecimal getBigDecimal(final RecordDefinition recordDefinition,
      final int fieldIndex, final ResultSet resultSet, final int parameterIndex,
      final boolean internStrings) throws SQLException {
      return resultSet.getBigDecimal(parameterIndex);
    }

    public static Boolean getBoolean(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getBoolean(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Byte getByte(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getByte(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Date getDate(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      return resultSet.getDate(parameterIndex);
    }

    public static Double getDouble(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getDouble(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Float getFloat(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getFloat(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Integer getInteger(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getInt(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Long getLong(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getLong(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static Object getObject(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      return resultSet.getObject(parameterIndex);
    }

    public static Short getShort(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      final var value = resultSet.getShort(parameterIndex);
      if (resultSet.wasNull()) {
        return null;
      } else {
        return value;
      }
    }

    public static String getString(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int parameterIndex, final boolean internStrings)
      throws SQLException {
      String value = resultSet.getString(parameterIndex);
      if (value != null && internStrings) {
        value = value.intern();
      }
      return value;
    }

    public static Timestamp getTimestamp(final RecordDefinition recordDefinition,
      final int fieldIndex, final ResultSet resultSet, final int parameterIndex,
      final boolean internStrings) throws SQLException {
      return resultSet.getTimestamp(parameterIndex);
    }

    public static void setBigDecimal(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      BigDecimal decimal;
      if (value instanceof final BigDecimal number) {
        decimal = number;
      } else if (value instanceof final BigInteger number) {
        decimal = new BigDecimal(number);
      } else if (value instanceof final Number number) {
        statement.setDouble(parameterIndex, number.doubleValue());
        return;
      } else {
        decimal = new BigDecimal(value.toString());
      }
      statement.setBigDecimal(parameterIndex, decimal);
    }

    public static void setBoolean(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      boolean booleanValue;
      if (value instanceof Boolean) {
        booleanValue = (Boolean)value;
      } else if (value instanceof Number) {
        final Number number = (Number)value;
        booleanValue = number.intValue() == 1;
      } else {
        final String stringValue = value.toString();
        if (stringValue.equals("1") || Boolean.parseBoolean(stringValue)) {
          booleanValue = true;
        } else {
          booleanValue = false;
        }
      }
      statement.setBoolean(parameterIndex, booleanValue);
    }

    public static void setByte(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      byte numberValue;
      if (value instanceof final Number number) {
        numberValue = number.byteValue();
      } else {
        numberValue = Byte.parseByte(value.toString());
      }
      statement.setByte(parameterIndex, numberValue);
    }

    public static void setDate(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      final var date = Dates.getSqlDate(value);
      statement.setDate(parameterIndex, date);
    }

    public static void setDouble(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      double numberValue;
      if (value instanceof final Number number) {
        numberValue = number.doubleValue();
      } else {
        numberValue = Double.parseDouble(value.toString());
      }
      statement.setDouble(parameterIndex, numberValue);
    }

    public static void setFloat(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      float numberValue;
      if (value instanceof final Number number) {
        numberValue = number.floatValue();
      } else {
        numberValue = Float.parseFloat(value.toString());
      }
      statement.setFloat(parameterIndex, numberValue);
    }

    public static void setInteger(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      int numberValue;
      if (value instanceof final Number number) {
        numberValue = number.intValue();
      } else {
        numberValue = Integer.parseInt(value.toString());
      }
      statement.setInt(parameterIndex, numberValue);
    }

    public static void setLong(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      long numberValue;
      if (value instanceof final Number number) {
        numberValue = number.longValue();
      } else {
        numberValue = Long.parseLong(value.toString());
      }
      statement.setLong(parameterIndex, numberValue);
    }

    public static void setObject(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      statement.setObject(parameterIndex, value);
    }

    public static void setShort(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      short numberValue;
      if (value instanceof final Number number) {
        numberValue = number.shortValue();
      } else {
        numberValue = Short.parseShort(value.toString());
      }
      statement.setShort(parameterIndex, numberValue);
    }

    public static void setString(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      final String string = value.toString();
      statement.setString(parameterIndex, string);
    }

    public static void setTimestamp(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException {
      final var timestamp = Dates.getTimestamp(value);
      statement.setTimestamp(parameterIndex, timestamp);
    }

  }

  static JdbcPreparedStatementValueHandler handler(Object value) {
    if (value instanceof TypedIdentifier) {
      return Handlers.HANDLER_STRING;
    } else if (value instanceof final Identifier identifier) {
      value = identifier.toSingleValue();
    }
    if (value == null) {
      return Handlers.HANDLER_OBJECT;
    } else if (value instanceof CharSequence) {
      return Handlers.HANDLER_STRING;
    } else if (value instanceof BigInteger) {
      return Handlers.HANDLER_LONG;
    } else if (value instanceof Long) {
      return Handlers.HANDLER_LONG;
    } else if (value instanceof Integer) {
      return Handlers.HANDLER_INTEGER;
    } else if (value instanceof Short) {
      return Handlers.HANDLER_SHORT;
    } else if (value instanceof Byte) {
      return Handlers.HANDLER_BYTE;
    } else if (value instanceof Double) {
      return Handlers.HANDLER_DOUBLE;
    } else if (value instanceof Float) {
      return Handlers.HANDLER_FLOAT;
    } else if (value instanceof BigDecimal) {
      return Handlers.HANDLER_BIG_DECIMAL;
    } else if (value instanceof Date) {
      return Handlers.HANDLER_DATE;
    } else if (value instanceof Instant) {
      return Handlers.HANDLER_TIMESTAMP;
    } else if (value instanceof java.util.Date) {
      return Handlers.HANDLER_TIMESTAMP;
    } else if (value instanceof Boolean) {
      return Handlers.HANDLER_BOOLEAN;
    } else {
      return Handlers.HANDLER_OBJECT;
    }
  }

  void addSelectStatementPlaceHolder(SqlAppendable sql);

  void appendSqlValue(SqlAppendable sql, RecordStore recordStore, Object queryValue);

  int setPreparedStatementValue(PreparedStatement statement, int parameterIndex, Object value)
    throws SQLException;

}
