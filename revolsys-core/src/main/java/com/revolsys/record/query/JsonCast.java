package com.revolsys.record.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.data.type.DataType;
import com.revolsys.jdbc.field.JdbcDataType;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class JsonCast extends AbstractUnaryQueryValue {
  private final String dataType;

  private JdbcDataType jdbcDataType;

  public JsonCast(final QueryValue queryValue, final String dataType) {
    super(queryValue);
    this.dataType = dataType;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("(");
    super.appendDefaultSql(statement, recordStore, buffer);
    buffer.append(" #>> '{}' )::");
    buffer.append(this.dataType);
  }

  @Override
  public JsonCast clone() {
    return (JsonCast)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof JsonCast) {
      final JsonCast condition = (JsonCast)obj;
      if (DataType.equal(condition.getDataType(), getDataType())) {
        return super.equals(condition);
      }
    }
    return false;
  }

  public String getDataType() {
    return this.dataType;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    if (this.jdbcDataType == null) {
      final var recordStore = (AbstractJdbcRecordStore)recordDefinition.getRecordStore();
      this.jdbcDataType = recordStore.dataTypeByName(this.dataType);
    }
    return this.jdbcDataType.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings);
  }

  @Override
  public String toString() {
    final StringBuilder buffer = new StringBuilder();
    buffer.append("CAST(");
    buffer.append(super.toString());
    buffer.append(" AS ");
    buffer.append(this.dataType);
    buffer.append(")");
    return buffer.toString();
  }
}
