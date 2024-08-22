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

import org.springframework.dao.DataAccessException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.JdbcDataSource;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.record.Record;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.StringBuilderSqlAppendable;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public interface JdbcRecordStore extends RecordStore {

  default void execteBatch(final PreparedStatement statement) throws SQLException {
    statement.executeBatch();
  }

  default int executeUpdate(final String sql, final Object... parameters) {
    return transactionCall(() -> {
      try (
          final JdbcConnection connection = getJdbcConnection()) {
        return connection.executeUpdate(sql, parameters);
      }
    });
  }

  JdbcDataSource getDataSource();

  default DataAccessException getException(final String task, final String sql,
      final SQLException e) {
    return getDataSource().getException(task, sql, e);
  }

  String getGeneratePrimaryKeySql(JdbcRecordDefinition recordDefinition);

  JdbcConnection getJdbcConnection();

  @Override
  default Record getRecord(final Query query) {
    return transactionCall(() -> RecordStore.super.getRecord(query));
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
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder("LOCK TABLE ");
    getRecordDefinition(typeName).appendFrom(sql);
    sql.append(" IN SHARE MODE");
    final String s = sql.toSqlString();
    try (
        final JdbcConnection connection = getJdbcConnection()) {
      connection.executeUpdate(s);
    } catch (final SQLException e) {
      throw getException("lock", s, e);
    }
  }

  default void lockTable(final String typePath) {
    final String tableName = JdbcUtils.getQualifiedTableName(typePath);
    final String sql = "LOCK TABLE " + tableName + " IN SHARE MODE";
    try (
        final JdbcConnection connection = getJdbcConnection()) {
      connection.executeUpdate(sql);
    } catch (final SQLException e) {
      throw getException("lock", sql, e);
    }
  }

  @Override
  JdbcRecordWriter newRecordWriter(RecordDefinitionProxy recordDefinition);

  JdbcRecordWriter newRecordWriter(RecordDefinitionProxy recordDefinition, int batchSize);

  default int selectInt(final String sql, final Object... parameters) {
    return transactionCall(() -> {
      try (
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
    });
  }

  default long selectLong(final String sql, final Object... parameters) {
    return transactionCall(() -> {
      try (
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
    });
  }

  default MapEx selectMap(final String sql, final Object... parameters) {
    return transactionCall(() -> {
      try (
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
    });
  }

  default String selectString(final String sql, final Object... parameters) throws SQLException {
    return transactionCall(() -> {
      try (
          JdbcConnection connection = getJdbcConnection()) {
        return JdbcUtils.selectString(connection, sql, parameters);
      }
    });
  }

  default int setRole(final String roleName) {
    final String sql = "SET ROLE " + roleName;
    try (
        final JdbcConnection connection = getJdbcConnection()) {
      try (
          Statement statement = connection.createStatement()) {
        return statement.executeUpdate(sql);
      }
    } catch (final SQLException e) {
      throw getException("Set role", sql, e);
    }
  }

  JdbcRecordStore setUseUpperCaseNames(boolean useUpperCaseNames);

  default Array toArray(final Connection connection, final JdbcFieldDefinition field,
      final Collection<?> value) throws SQLException {
    throw new UnsupportedOperationException("Conversion to array not yet supported");
  }

}
