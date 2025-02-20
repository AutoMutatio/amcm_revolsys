package com.revolsys.record.query;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.data.type.DataType;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class Cast extends AbstractUnaryQueryValue {
  private final String dataType;

  private JdbcFieldDefinition convertedField;

  public Cast(final QueryValue queryValue, final String dataType) {
    super(queryValue);
    this.dataType = dataType;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
      final SqlAppendable buffer) {
    buffer.append("CAST(");
    super.appendDefaultSql(statement, recordStore, buffer);
    buffer.append(" AS ");
    buffer.append(this.dataType);
    buffer.append(")");
  }

  @Override
  public Cast clone() {
    return (Cast) super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Cast) {
      final Cast condition = (Cast) obj;
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
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final ResultSet resultSet,
      final ColumnIndexes indexes,
      final boolean internStrings) throws SQLException {
    if (this.convertedField == null) {
      final var recordStore = (AbstractJdbcRecordStore) recordDefinition.getRecordStore();
      this.convertedField = recordStore.getFieldAdder(this.dataType).newField(null, null, "", "", 0, this.dataType, 0,
          0, false, null);
    }
    return this.convertedField.getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
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
