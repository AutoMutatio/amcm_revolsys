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
import java.util.Collection;
import java.util.function.Supplier;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.TypedIdentifier;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypeValueFactory;
import com.revolsys.data.type.DataTypes;
import com.revolsys.date.Dates;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public interface JdbcDataType extends DataType {

  public static class Util {
    private static JdbcDataTypeRegistry REGISTRY = new JdbcDataTypeRegistry();

    static {
      for (final var field : JdbcDataType.class.getDeclaredFields()) {
        try {
          final var value = field.get(null);
          if (value instanceof final JdbcDataType type) {
            REGISTRY.addType(type);
          }
        } catch (IllegalArgumentException | IllegalAccessException e) {
          throw Exceptions.toRuntimeException(e);
        }
      }
    }

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

  JdbcDataType BIG_DECIMAL = new LambaJdbcDataType(DataTypes.DECIMAL, Types.NUMERIC,
    Util::getBigDecimal, Util::setBigDecimal);

  JdbcDataType BOOLEAN = new LambaJdbcDataType(DataTypes.BOOLEAN, Types.BOOLEAN, Util::getBoolean,
    Util::setBoolean);

  JdbcDataType BYTE = new LambaJdbcDataType(DataTypes.BYTE, Types.TINYINT, Util::getByte,
    Util::setByte);

  JdbcDataType DATE = new LambaJdbcDataType(DataTypes.SQL_DATE, Types.DATE, Util::getDate,
    Util::setDate);

  JdbcDataType DOUBLE = new LambaJdbcDataType(DataTypes.DOUBLE, Types.DOUBLE, Util::getDouble,
    Util::setDouble);

  JdbcDataType FLOAT = new LambaJdbcDataType(DataTypes.FLOAT, Types.FLOAT, Util::getFloat,
    Util::setFloat);

  JdbcDataType INTEGER = new LambaJdbcDataType(DataTypes.INT, Types.INTEGER, Util::getInteger,
    Util::setInteger);

  JdbcDataType LONG = new LambaJdbcDataType(DataTypes.LONG, Types.BIGINT, Util::getLong,
    Util::setLong);

  JdbcDataType OBJECT = createObject(DataTypes.OBJECT);

  JdbcDataType SHORT = new LambaJdbcDataType(DataTypes.SHORT, Types.SMALLINT, Util::getShort,
    Util::setShort);

  JdbcDataType STRING = new LambaJdbcDataType(DataTypes.STRING, Types.CHAR, Util::getString,
    Util::setString);

  JdbcDataType TIMESTAMP = new LambaJdbcDataType(DataTypes.TIMESTAMP, Types.TIMESTAMP,
    Util::getTimestamp, Util::setTimestamp);

  static JdbcDataType byJavaClass(final Class<?> javaClass) {
    return Util.REGISTRY.byJavaClass(javaClass);
  }

  static JdbcDataType byName(final String name) {
    return Util.REGISTRY.byName(name);
  }

  static JdbcDataType bySqlType(final int sqlType) {
    return Util.REGISTRY.bySqlType(sqlType);
  }

  static JdbcDataType byType(final DataType dataType) {
    return Util.REGISTRY.byType(dataType);
  }

  static LambaJdbcDataType createObject(final DataType dataType) {
    return new LambaJdbcDataType(dataType, Types.OTHER, Util::getObject, Util::setObject);
  }

  static JdbcDataType dataType(Object value) {
    if (value instanceof TypedIdentifier) {
      return JdbcDataType.STRING;
    } else if (value instanceof final Identifier identifier) {
      value = identifier.toSingleValue();
    }
    if (value == null) {
      return JdbcDataType.OBJECT;
    } else if (value instanceof CharSequence) {
      return JdbcDataType.STRING;
    } else if (value instanceof BigInteger) {
      return JdbcDataType.LONG;
    } else if (value instanceof Long) {
      return JdbcDataType.LONG;
    } else if (value instanceof Integer) {
      return JdbcDataType.INTEGER;
    } else if (value instanceof Short) {
      return JdbcDataType.SHORT;
    } else if (value instanceof Byte) {
      return JdbcDataType.BYTE;
    } else if (value instanceof Double) {
      return JdbcDataType.DOUBLE;
    } else if (value instanceof Float) {
      return JdbcDataType.FLOAT;
    } else if (value instanceof BigDecimal) {
      return JdbcDataType.BIG_DECIMAL;
    } else if (value instanceof Date) {
      return JdbcDataType.DATE;
    } else if (value instanceof Instant) {
      return JdbcDataType.TIMESTAMP;
    } else if (value instanceof java.util.Date) {
      return JdbcDataType.TIMESTAMP;
    } else if (value instanceof Boolean) {
      return JdbcDataType.BOOLEAN;
    } else {
      return JdbcDataType.OBJECT;
    }
  }

  /**
   * Create a new registry that uses the data types defined on this interface as a default registry.
   *
   * @return
   */
  public static JdbcDataTypeRegistry newRegistry() {
    return Util.REGISTRY.newRegistry();
  }

  void addSelectStatementPlaceHolder(SqlAppendable sql);

  void appendSqlValue(SqlAppendable sql, RecordStore recordStore, Object queryValue);

  @Override
  default int compareNullFirst(final Object object1, final Object object2) {
    return dataType().compareNullFirst(object1, object2);
  }

  DataType dataType();

  @Override
  default boolean equals(final Object object1, final Object object2) {
    return dataType().equals(object1, object2);
  }

  @Override
  default boolean equals(final Object object1, final Object object2,
    final Collection<? extends CharSequence> excludeFieldNames) {
    return dataType().equals(object1, object2, excludeFieldNames);
  }

  @Override
  default boolean equalsExact(final Object value1, final Object value2) {
    return DataType.equalExact(value1, value2);
  }

  @Override
  default Class<?> getJavaClass() {
    return dataType().getJavaClass();
  }

  @Override
  default <V> V getMaxValue() {
    return dataType().getMaxValue();
  }

  @Override
  default <V> V getMinValue() {
    return dataType().getMinValue();
  }

  @Override
  default String getName() {
    return dataType().getName();
  }

  @Override
  default String getValidationName() {
    return dataType().getValidationName();
  }

  Object getValueFromResultSet(RecordDefinition recordDefinition, int fieldIndex,
    ResultSet resultSet, ColumnIndexes indexes, boolean internStrings) throws SQLException;

  @Override
  default boolean isAssignableFrom(final Class<?> valueClass) {
    return dataType().isAssignableFrom(valueClass);
  }

  @Override
  default boolean isAssignableTo(final Class<?> valueClass) {
    return dataType().isAssignableTo(valueClass);
  }

  @Override
  default boolean isInstance(final Object value) {
    return dataType().isInstance(value);
  }

  @Override
  default boolean isMathSupported() {
    return dataType().isMathSupported();
  }

  @Override
  default boolean isNumeric() {
    return dataType().isNumeric();
  }

  @Override
  default boolean isRequiresQuotes() {
    // TODO Auto-generated method stub
    return dataType().isRequiresQuotes();
  }

  @Override
  default boolean isValid(final Object value) {
    return dataType().isValid(value);
  }

  @Override
  default <V> DataTypeValueFactory<V> newFactory(final Supplier<V> supplier) {
    return dataType().newFactory(supplier);
  }

  @Override
  default <V> V plus(final Object value, final Number number) {
    return dataType().plus(value, number);
  }

  int setPreparedStatementValue(PreparedStatement statement, int parameterIndex, Object value)
    throws SQLException;

  int sqlType();

  @Override
  default <V> V toObject(final Object value) {
    return dataType().toObject(value);
  }

  @Override
  default <V> V toObject(final String value) {
    return dataType().toObject(value);
  }

  @Override
  default String toString(final Object value) {
    return dataType().toString(value);
  }
}
