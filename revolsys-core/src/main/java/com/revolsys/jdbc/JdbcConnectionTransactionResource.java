package com.revolsys.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import com.revolsys.jdbc.JdbcDataSource.ConnectionConsumer;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.transaction.Isolation;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.transaction.TransactionableResource;

public abstract class JdbcConnectionTransactionResource implements TransactionableResource {
  private final ReentrantLockEx lock = new ReentrantLockEx();

  private Connection connection;

  private final TransactionContext context;

  private ConnectionConsumer connectionInitializer = ConnectionConsumer.EMPTY;

  private boolean hasError = false;

  public JdbcConnectionTransactionResource(final TransactionContext context) {
    this.context = context;
  }

  public JdbcConnectionTransactionResource addConnectionInitializer(
    final ConnectionConsumer initializer) {
    if (initializer != null) {
      if (this.connection == null) {
        if (this.connectionInitializer == ConnectionConsumer.EMPTY) {
          this.connectionInitializer = initializer;
        } else {
          this.connectionInitializer = this.connectionInitializer.andThen(initializer);
        }
      } else {
        try {
          initializer.accept(this.connection);
        } catch (final SQLException e) {
          throw getDataSource().getException("initialize", null, e);
        }
      }
    }
    return this;
  }

  @Override
  public void close() {
    final Connection connection = this.connection;
    if (connection != null) {
      try {
        if (this.context.isReadOnly() || connection.isReadOnly()) {
          connection.setReadOnly(false);
        }
      } catch (final SQLException e) {
        setHasError();
      }
      try {
        closeConnection();
      } catch (final SQLException e) {
        throw getDataSource().getException("close", null, e);
      }
    }
  }

  protected void closeConnection() throws SQLException {
    this.connection.close();
  }

  @Override
  public void commit() {
    final Connection connection = this.connection;
    if (connection != null) {
      try {
        connection.commit();
      } catch (final SQLException e) {
        throw getDataSource().getException("commit", null, e);
      }
    }
  }

  protected Connection getConnection() {
    return this.connection;
  }

  protected abstract JdbcDataSource getDataSource();

  public JdbcConnection getJdbcConnection() throws SQLException {
    try (
      var l = this.lock.lockX()) {
      if (this.connection == null) {
        this.connection = newConnection();
      }
      return newJdbcConnection(this.connection);
    }
  }

  private void initDefaultConnection(final Connection connection) throws SQLException {
    // Disable auto commit
    if (connection.getAutoCommit()) {
      connection.setAutoCommit(false);
    }

    // Set read-only
    if (this.context.isReadOnly()) {
      connection.setReadOnly(true);
    }

    // Set isolation
    final Isolation isolation = this.context.getIsolation();
    if (!Isolation.DEFAULT.equals(isolation)) {
      final int currentIsolation = connection.getTransactionIsolation();
      final int isolationLevel = isolation.value();
      if (currentIsolation != isolationLevel) {
        connection.setTransactionIsolation(isolationLevel);
      }
    }
  }

  protected void initializeConnection(final Connection connection) throws SQLException {
    initDefaultConnection(connection);
    this.connectionInitializer.accept(connection);
  }

  public boolean isHasError() {
    return this.hasError;
  }

  protected abstract Connection newConnection() throws SQLException;

  protected abstract JdbcConnection newJdbcConnection(Connection connection) throws SQLException;

  @Override
  public void resume() {
  }

  @Override
  public void rollback() {
    final Connection connection = this.connection;
    if (connection != null) {
      try {
        connection.rollback();
      } catch (final SQLException e) {
        throw getDataSource().getException("rollback", null, e);
      }
    }
  }

  @Override
  public void setHasError() {
    this.hasError = true;
  }

  @Override
  public void suspend() {

  }
}
