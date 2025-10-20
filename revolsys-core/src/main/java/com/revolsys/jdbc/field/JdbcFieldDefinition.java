package com.revolsys.jdbc.field;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;

import com.revolsys.data.type.DataType;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;
import com.revolsys.record.Record;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class JdbcFieldDefinition extends FieldDefinition {
  private String dbName;

  private boolean quoteName = false;

  private int sqlType;

  private String dbDataType;

  JdbcFieldDefinition() {
    setName(JdbcFieldDefinitions.UNKNOWN);
  }

  public JdbcFieldDefinition(final String dbName, final String name, final DataType type,
    final int sqlType, final String dbDataType, final int length, final int scale,
    final boolean required, final String description, final Map<String, Object> properties) {
    super(name, type, length, scale, required, description, properties);
    this.dbName = dbName;
    this.sqlType = sqlType;
    this.dbDataType = dbDataType;
  }

  @Override
  public JdbcFieldDefinition addField(final AbstractJdbcRecordStore recordStore,
    final JdbcRecordDefinition recordDefinition, final ResultSetMetaData metaData,
    final ColumnIndexes columnIndexes) throws SQLException {
    final int columnIndex = columnIndexes.incrementAndGet();
    final String baseName = metaData.getColumnName(columnIndex);
    var name = baseName;
    var i = 0;
    while (recordDefinition.hasField(name)) {
      name = baseName + (++i);
    }
    final JdbcFieldDefinition newField = clone();
    newField.setName(name);
    recordDefinition.addField(newField);
    return newField;
  }

  public void addInsertStatementPlaceHolder(final SqlAppendable sql, final boolean generateKeys) {
    addStatementPlaceHolder(sql);
  }

  public void addSelectStatementPlaceHolder(final SqlAppendable sql) {
    addStatementPlaceHolder(sql);
  }

  public void addStatementPlaceHolder(final SqlAppendable sql) {
    sql.append('?');
  }

  @Override
  public void appendColumnName(final SqlAppendable sql) {
    appendColumnName(sql, this.quoteName);
  }

  @Override
  public void appendColumnName(final SqlAppendable sql, boolean quoteName) {
    quoteName |= this.quoteName;
    if (quoteName) {
      sql.append('"');
    }
    final String dbName = getDbName();
    sql.append(dbName);
    if (quoteName) {
      sql.append('"');
    }
  }

  public void appendSqlValue(final SqlAppendable sql, final RecordStore recordStore,
    final Object queryValue) {
    if (recordStore == null) {
      RecordStore.appendDefaultSql(sql, queryValue);
    } else {
      recordStore.appendSqlValue(sql, queryValue);
    }
  }

  @Override
  public JdbcFieldDefinition clone() {
    final JdbcFieldDefinition clone = new JdbcFieldDefinition(this.dbName, getName(), getDataType(),
      getSqlType(), getDbDataType(), getLength(), getScale(), isRequired(), getDescription(),
      getProperties());
    postClone(clone);
    return clone;
  }

  public String getDbDataType() {
    return this.dbDataType;
  }

  public String getDbName() {
    return this.dbName;
  }

  public int getSqlType() {
    return this.sqlType;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return resultSet.getObject(indexes.incrementAndGet());
  }

  public boolean isQuoteName() {
    return this.quoteName;
  }

  @Override
  public boolean isSortable() {
    switch (this.sqlType) {
      case Types.ARRAY:
      case Types.BLOB:
      case Types.CLOB:
      case Types.JAVA_OBJECT:
      case Types.OTHER:
      case Types.STRUCT:
      case Types.SQLXML:
        return false;
      default:
        return true;
    }
  }

  protected void postClone(final JdbcFieldDefinition clone) {
    super.postClone(clone);
    clone.dbName = this.dbName;
    clone.quoteName = this.quoteName;
    clone.sqlType = this.sqlType;
  }

  @Override
  public JdbcFieldDefinition setGenerated(final boolean generated) {
    super.setGenerated(generated);
    if (generated) {
      ((JdbcRecordDefinition)getRecordDefinition()).setHasGeneratedFields(true);
    }
    return this;
  }

  public int setInsertPreparedStatementValue(final PreparedStatement statement,
    final int parameterIndex, final Object value) throws SQLException {
    return setPreparedStatementValue(statement, parameterIndex, value);
  }

  public int setInsertPreparedStatementValue(final PreparedStatement statement,
    final int parameterIndex, final Record record) throws SQLException {
    final String name = getName();
    final Object value = record.getValue(name);
    return setInsertPreparedStatementValue(statement, parameterIndex, value);
  }

  public int setPreparedStatementArray(final PreparedStatement statement, final int parameterIndex,
    final List<?> values) throws SQLException {
    throw new UnsupportedOperationException();
  }

  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      statement.setNull(parameterIndex, this.sqlType);
    } else {
      statement.setObject(parameterIndex, value);
    }
    return parameterIndex + 1;
  }

  public void setQuoteName(final boolean quoteName) {
    this.quoteName = quoteName;
  }

  public void setSqlType(final int sqlType) {
    this.sqlType = sqlType;
  }

}
