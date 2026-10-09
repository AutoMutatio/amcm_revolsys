package com.revolsys.jdbc.field;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class LambaJdbcPreparedStatementValueHandler implements JdbcPreparedStatementValueHandler {

  public interface GetHandler {
    Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
      final ResultSet resultSet, final int index, final boolean internStrings) throws SQLException;
  }

  public interface SetHandler {
    void setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
      final Object value) throws SQLException;
  }

  private final int sqlType;

  private final SetHandler setValue;

  private GetHandler getValue;

  public LambaJdbcPreparedStatementValueHandler(final int sqlType, final GetHandler getValue,
    final SetHandler setValue) {
    this.sqlType = sqlType;
    this.getValue = getValue;
    this.setValue = setValue;
  }

  public LambaJdbcPreparedStatementValueHandler(final int sqlType, final SetHandler setValue) {
    this.sqlType = sqlType;
    this.setValue = setValue;
  }

  @Override
  public void addSelectStatementPlaceHolder(final SqlAppendable sql) {
    sql.append('?');
  }

  @Override
  public void appendSqlValue(final SqlAppendable sql, final RecordStore recordStore,
    final Object queryValue) {
    if (recordStore == null) {
      RecordStore.appendDefaultSql(sql, queryValue);
    } else {
      recordStore.appendSqlValue(sql, queryValue);
    }
  }

  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final int parameterIndex = indexes.incrementAndGet();
    return this.getValue.getValueFromResultSet(recordDefinition, fieldIndex, resultSet,
      parameterIndex, internStrings);
  }

  @Override
  public int setPreparedStatementValue(final PreparedStatement statement, final int parameterIndex,
    final Object value) throws SQLException {
    if (value == null) {
      statement.setNull(parameterIndex, this.sqlType);
    } else {
      this.setValue.setPreparedStatementValue(statement, parameterIndex, value);
    }
    return parameterIndex + 1;
  }

}
