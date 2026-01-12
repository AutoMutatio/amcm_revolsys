package com.revolsys.gis.postgresql.type;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import org.postgresql.util.PGobject;
import org.postgresql.util.PGtokenizer;

import com.revolsys.data.type.DataTypes;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.schema.RecordDefinition;

public class PostgreSQLStructFieldDefinition extends JdbcFieldDefinition {

  public PostgreSQLStructFieldDefinition(final String dbName, final String name, final int sqlType,
    final String dbDataType, final int length, final int scale, final boolean required,
    final String description, final Map<String, Object> properties) {
    super(dbName, name, DataTypes.OBJECT, sqlType, dbDataType, length, scale, required, description,
      properties);
  }

  @Override
  public JdbcFieldDefinition clone() {
    final PostgreSQLStructFieldDefinition clone = new PostgreSQLStructFieldDefinition(getDbName(),
      getName(), getSqlType(), getDbDataType(), getLength(), getScale(), isRequired(),
      getDescription(), getProperties());
    postClone(clone);
    return clone;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final var object = (PGobject)resultSet.getObject(indexes.incrementAndGet());
    final String value = object.getValue();
    final var tokenizer = new PGtokenizer(value.substring(1, value.length() - 1), ',');

    final PathName typeName = PathName.fromDotSeparated(getDbDataType().replaceAll("\"", ""));
    final var fieldRecordDefinition = recordDefinition.getRecordStore()
      .getRecordDefinition(typeName);

    // This doesn't fully work
    final var record = fieldRecordDefinition.newRecord();
    for (int i = 0; i < tokenizer.getSize(); i++) {
      final String fieldValue = tokenizer.getToken(i);
      record.setValue(i, fieldValue);
    }
    return record;
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      final int sqlType = getSqlType();
      statement.setNull(parameterIndex, sqlType);
    } else {
      throw new UnsupportedOperationException();
    }
    return parameterIndex + 1;

  }

  @Override
  public <V> V toFieldValueException(final Object value) {
    throw new UnsupportedOperationException();
  }

}
