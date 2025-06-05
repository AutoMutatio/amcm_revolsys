package com.revolsys.gis.postgresql.type;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.postgresql.util.PGobject;

import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonType;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class PostgreSQLJsonbFieldDefinition extends JdbcFieldDefinition {

  public PostgreSQLJsonbFieldDefinition(final String dbName, final String name, final int sqlType,
    final String dbDataType, final int length, final int scale, final boolean required,
    final String description, final Map<String, Object> properties) {
    super(
      dbName,
        name,
        Json.JSON_TYPE,
        sqlType,
        dbDataType,
        length,
        scale,
        required,
        description,
        properties);
  }

  @Override
  public JdbcFieldDefinition clone() {
    final PostgreSQLJsonbFieldDefinition clone = new PostgreSQLJsonbFieldDefinition(getDbName(),
      getName(), getSqlType(), getDbDataType(), getLength(), getScale(), isRequired(),
      getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    String value = resultSet.getString(indexes.incrementAndGet());
    if (value != null && internStrings) {
      value = value.intern();
    }
    return toColumnType(value);
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      final PGobject json = new PGobject();
      json.setType("jsonb");
      final String string = Json.toJsonString(value);
      json.setValue(string);
      statement.setObject(parameterIndex, json);
    }
    return parameterIndex + 1;

  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V toFieldValue(final Object value) {
    if (value instanceof final Number number) {
      return (V)number;
    } else if (value instanceof final JsonType json) {
      return (V)json;
    } else if (value instanceof final String string) {
      final char firstChar = string.charAt(0);
      return (V)switch (firstChar) {
        case '[': {
          yield Json.parse(string);
        }
        case '{': {
          yield Json.parse(string);
        }
        default:
        yield string;
      };
    }
    return super.toFieldValue(value);
  }
}
