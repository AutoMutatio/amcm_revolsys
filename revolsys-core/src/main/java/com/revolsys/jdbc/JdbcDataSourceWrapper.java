package com.revolsys.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.util.BaseCloseable;

public class JdbcDataSourceWrapper extends JdbcDataSource {

  private class ConnectionTransactionResource extends JdbcConnectionTransactionResource {

    public ConnectionTransactionResource(final TransactionContext context) {
      super(context);
    }

    @Override
    protected JdbcDataSourceWrapper getDataSource() {
      return JdbcDataSourceWrapper.this;
    }

    @Override
    protected Connection newConnection() throws SQLException {
      final Connection connection = newConnectionInternal();
      try {
        initializeConnection(connection);
      } catch (SQLException | RuntimeException | Error e) {
        BaseCloseable.closeSilent(connection);
        throw e;
      }
      return connection;
    }

    @Override
    protected JdbcConnection newJdbcConnection(final Connection connection) throws SQLException {
      return new JdbcConnection(JdbcDataSourceWrapper.this, connection, false);
    }
  }

  private final DataSource dataSource;

  public JdbcDataSourceWrapper(final DataSource dataSource) {
    super();
    this.dataSource = dataSource;
  }

  @Override
  public Connection getConnection(final String username, final String password)
      throws SQLException {
    throw new UnsupportedOperationException("Username/password connections are not supported");
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.dataSource.getLoginTimeout();
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.dataSource.getLogWriter();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.dataSource.getParentLogger();
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return this.dataSource.isWrapperFor(iface);
  }

  private Connection newConnectionInternal() {
    try {
      final Connection connection = this.dataSource.getConnection();
      if (connection == null) {
        throw new CannotGetJdbcConnectionException("Data source returned null connection");
      }
      return connection;
    } catch (final SQLException e) {
      throw getException("New connection", null, e);
    }
  }

  @Override
  protected JdbcConnectionTransactionResource newConnectionTransactionResource(
      final ActiveTransactionContext context) {
    return new ConnectionTransactionResource(context);
  }

  @Override
  protected SQLErrorCodeSQLExceptionTranslator newExceptionTranslator() {
    try {
      return new SQLErrorCodeSQLExceptionTranslator(this.dataSource);
    } catch (final Exception e) {
      return new SQLErrorCodeSQLExceptionTranslator();
    }
  }

  @Override
  protected JdbcConnection newJdbcConnection(JsonObject properties) throws SQLException {
    final var connection = newConnectionInternal();
    return new JdbcConnection(this, connection, true);
  }

  @Override
  public void setLoginTimeout(final int seconds) throws SQLException {
    this.dataSource.setLoginTimeout(seconds);
  }

  @Override
  public void setLogWriter(final PrintWriter out) throws SQLException {
    this.dataSource.setLogWriter(out);
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    return this.dataSource.unwrap(iface);
  }
}
