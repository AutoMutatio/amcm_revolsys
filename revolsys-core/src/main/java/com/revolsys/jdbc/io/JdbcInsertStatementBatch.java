package com.revolsys.jdbc.io;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.record.query.InsertStatement;
import com.revolsys.record.query.InsertStatement.InsertStatementBatch;

public class JdbcInsertStatementBatch implements InsertStatementBatch {

  private final InsertStatement insertStatement;

  private final PreparedStatement statement;

  private final JdbcConnection connection;

  public JdbcInsertStatementBatch(InsertStatement insertStatement, JdbcConnection connection,
    PreparedStatement statement) {
    this.connection = connection;
    this.insertStatement = insertStatement;
    this.statement = statement;
  }

  @Override
  public JdbcInsertStatementBatch addRowToBatch(Map<String, Object> values) {
    try {
      this.insertStatement.appendParameters(1, values, statement);
      statement.addBatch();
      return this;
    } catch (final SQLException e) {
      final var sqlString = insertStatement.toString();
      throw connection.getException("addBatch", sqlString, e);
    }
  }

  @Override
  public int[] executeBatch() {
    try {
      return statement.executeBatch();
    } catch (final SQLException e) {
      final var sqlString = insertStatement.toString();
      throw connection.getException("executeBatch", sqlString, e);
    }
  }

}
