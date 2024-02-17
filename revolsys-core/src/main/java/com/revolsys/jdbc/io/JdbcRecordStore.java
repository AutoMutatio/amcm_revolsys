package com.revolsys.jdbc.io;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

import com.revolsys.collection.map.MapEx;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.StringBuilderSqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.transaction.Propagation;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionOptions;

public interface JdbcRecordStore extends RecordStore {

  default void execteBatch(final PreparedStatement statement) throws SQLException {
    statement.executeBatch();
  }

  default int executeUpdate(final String sql, final Object... parameters) {
    try (
      Transaction transaction = newTransaction(Propagation.REQUIRED);
      final JdbcConnection connection = getJdbcConnection()) {
      return connection.executeUpdate(sql, parameters);
    }
  }

  String getGeneratePrimaryKeySql(JdbcRecordDefinition recordDefinition);

  JdbcConnection getJdbcConnection();

  JdbcConnection getJdbcConnection(boolean autoCommit);

  @Override
  default Record getRecord(final Query query) {
    try (
      Transaction transaction = newTransaction(TransactionOptions.REQUIRED)) {
      return RecordStore.super.getRecord(query);
    }
  }

  JdbcRecordDefinition getRecordDefinition(PathName tablePath, ResultSetMetaData resultSetMetaData,
    String dbTableName);

  JdbcRecordDefinition getRecordDefinition(Query query, ResultSetMetaData resultSetMetaData);

  default ResultSet getResultSet(final PreparedStatement statement, final Query query)
    throws SQLException {
    query.appendParameters(1, statement);
    return statement.executeQuery();
  }

  PreparedStatement insertStatementPrepareRowId(JdbcConnection connection,
    RecordDefinition recordDefinition, String sql) throws SQLException;

  boolean isIdFieldRowid(RecordDefinition recordDefinition);

  default void lockTable(final PathName typeName) {
    try (
      final JdbcConnection connection = getJdbcConnection()) {
      final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder("LOCK TABLE ");
      getRecordDefinition(typeName).appendFrom(sql);
      sql.append(" IN SHARE MODE");
      connection.executeUpdate(sql.toSqlString());
    }
  }

  default void lockTable(final String typePath) {
    try (
      final JdbcConnection connection = getJdbcConnection()) {
      final String tableName = JdbcUtils.getQualifiedTableName(typePath);
      final String sql = "LOCK TABLE " + tableName + " IN SHARE MODE";
      connection.executeUpdate(sql);
    }
  }

  default int selectInt(final String sql, final Object... parameters) {
    try (
      Transaction transaction = newTransaction(Propagation.REQUIRED);
      JdbcConnection connection = getJdbcConnection()) {
      try (
        final PreparedStatement statement = connection.prepareStatement(sql)) {
        JdbcUtils.setParameters(statement, parameters);

        try (
          final ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            return resultSet.getInt(1);
          } else {
            throw new IllegalArgumentException("Value not found");
          }
        }
      } catch (final SQLException e) {
        throw connection.getException("selectInt", sql, e);
      }
    }
  }

  default long selectLong(final String sql, final Object... parameters) {
    try (
      Transaction transaction = newTransaction(Propagation.REQUIRED);
      JdbcConnection connection = getJdbcConnection()) {
      try (
        final PreparedStatement statement = connection.prepareStatement(sql)) {
        JdbcUtils.setParameters(statement, parameters);

        try (
          final ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            return resultSet.getLong(1);
          } else {
            throw new IllegalArgumentException("Value not found");
          }
        }
      } catch (final SQLException e) {
        throw connection.getException("selectInt", sql, e);
      }
    }
  }

  default MapEx selectMap(final String sql, final Object... parameters) {
    try (
      Transaction transaction = newTransaction(Propagation.REQUIRED);
      JdbcConnection connection = getJdbcConnection()) {
      try (
        final PreparedStatement statement = connection.prepareStatement(sql)) {
        JdbcUtils.setParameters(statement, parameters);

        try (
          final ResultSet resultSet = statement.executeQuery()) {
          if (resultSet.next()) {
            return JdbcUtils.readMap(resultSet);
          } else {
            throw new IllegalArgumentException(
              "Value not found for " + sql + " " + Arrays.asList(parameters));
          }
        }
      } catch (final SQLException e) {
        throw connection.getException(null, sql, e);
      }
    }
  }

  default String selectString(final String sql, final Object... parameters) throws SQLException {
    try (
      Transaction transaction = newTransaction(Propagation.REQUIRED);
      JdbcConnection connection = getJdbcConnection()) {
      return JdbcUtils.selectString(connection, sql, parameters);
    }
  }

  default int setRole(final String roleName) {
    return executeUpdate("SET ROLE " + roleName);
  }

  default int setRole(final Transaction transaction, final String roleName) {
    try (
      final JdbcConnection connection = getJdbcConnection()) {
      final String sql = "SET ROLE " + roleName;
      try (
        Statement statement = connection.createStatement()) {
        return statement.executeUpdate(sql);
      } catch (final SQLException e) {
        throw connection.getException("Set role", sql, e);
      }
    }
  }

  JdbcRecordStore setUseUpperCaseNames(boolean useUpperCaseNames);

  default Array toArray(final Connection connection, final JdbcFieldDefinition field,
    final Collection<?> value) throws SQLException {
    throw new UnsupportedOperationException("Conversion to array not yet supported");
  }

}
