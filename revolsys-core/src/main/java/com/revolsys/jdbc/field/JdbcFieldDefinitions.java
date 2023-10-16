package com.revolsys.jdbc.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Types;
import java.time.Instant;

import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.identifier.TypedIdentifier;
import com.revolsys.data.type.DataTypes;

public class JdbcFieldDefinitions {

  public static final String UNKNOWN = "UNKNOWN";

  private static final JdbcFieldDefinition FIELD_UNKNOWN = new JdbcFieldDefinition();

  private static final JdbcBooleanFieldDefinition FIELD_BOOLEAN = new JdbcBooleanFieldDefinition(
    UNKNOWN, UNKNOWN, Types.BIT, "bit", -1, false, null, null);

  private static final JdbcTimestampFieldDefinition FIELD_TIMESTAMP = new JdbcTimestampFieldDefinition(
    UNKNOWN, UNKNOWN, -1, "timestamp", false, null, null);

  static final JdbcDateFieldDefinition FIELD_DATE = new JdbcDateFieldDefinition(UNKNOWN, UNKNOWN,
    -1, "date", false, null, null);

  private static final JdbcBigDecimalFieldDefinition FIELD_BIG_DECIMAL = new JdbcBigDecimalFieldDefinition(
    UNKNOWN, UNKNOWN, Types.NUMERIC, "numeric", -1, -1, false, null, null);

  private static final JdbcFloatFieldDefinition FIELD_FLOAT = new JdbcFloatFieldDefinition(UNKNOWN,
    UNKNOWN, Types.FLOAT, "float", false, null, null);

  private static final JdbcDoubleFieldDefinition FIELD_DOUBLE = new JdbcDoubleFieldDefinition(
    UNKNOWN, UNKNOWN, Types.DOUBLE, "double", false, null, null);

  private static final JdbcByteFieldDefinition FIELD_BYTE = new JdbcByteFieldDefinition(UNKNOWN,
    UNKNOWN, Types.TINYINT, "tinyint", false, null, null);

  private static final JdbcShortFieldDefinition FIELD_SHORT = new JdbcShortFieldDefinition(UNKNOWN,
    UNKNOWN, Types.SMALLINT, "smallint", false, null, null);

  private static final JdbcIntegerFieldDefinition FIELD_INTEGER = new JdbcIntegerFieldDefinition(
    UNKNOWN, UNKNOWN, Types.INTEGER, "int", false, null, null);

  private static final JdbcLongFieldDefinition FIELD_LONG = new JdbcLongFieldDefinition(UNKNOWN,
    UNKNOWN, Types.BIGINT, "integer", false, null, null);

  private static final JdbcStringFieldDefinition FIELD_STRING = new JdbcStringFieldDefinition(
    UNKNOWN, UNKNOWN, Types.CHAR, "char", -1, false, null, null);

  private static final JdbcFieldDefinition FIELD_OBJECT = new JdbcFieldDefinition(UNKNOWN, UNKNOWN,
    DataTypes.OBJECT, Types.OTHER, "other", 0, 0, false, null, null);

  public static JdbcFieldDefinition newFieldDefinition(Object value) {
    if (value instanceof TypedIdentifier) {
      return FIELD_STRING;
    } else if (value instanceof Identifier) {
      final Identifier identifier = (Identifier)value;
      value = identifier.toSingleValue();
    }
    if (value == null) {
      return FIELD_OBJECT;
    } else if (value instanceof CharSequence) {
      return FIELD_STRING;
    } else if (value instanceof BigInteger) {
      return FIELD_LONG;
    } else if (value instanceof Long) {
      return FIELD_LONG;
    } else if (value instanceof Integer) {
      return FIELD_INTEGER;
    } else if (value instanceof Short) {
      return FIELD_SHORT;
    } else if (value instanceof Byte) {
      return FIELD_BYTE;
    } else if (value instanceof Double) {
      return FIELD_DOUBLE;
    } else if (value instanceof Float) {
      return FIELD_FLOAT;
    } else if (value instanceof BigDecimal) {
      return FIELD_BIG_DECIMAL;
    } else if (value instanceof Date) {
      return FIELD_DATE;
    } else if (value instanceof Instant) {
      return FIELD_TIMESTAMP;
    } else if (value instanceof java.util.Date) {
      return FIELD_TIMESTAMP;
    } else if (value instanceof Boolean) {
      return FIELD_BOOLEAN;
    } else {
      return FIELD_UNKNOWN;
    }
  }

}
