package com.revolsys.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import com.revolsys.jdbc.JdbcDataSource.ConnectionConsumer;
import com.revolsys.parallel.ThreadUtil;
import com.revolsys.transaction.Isolation;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.transaction.TransactionableResource;
import com.revolsys.util.Debug;

public class JdbcConnectionTransactionResource implements TransactionableResource {
  private final ReentrantLock lock = new ReentrantLock();

  private Connection connection;

  private final JdbcDataSource dataSource;

  private final TransactionContext context;

  private List<ConnectionConsumer> connectionInitializers;

  public JdbcConnectionTransactionResource(final TransactionContext context,
    final JdbcDataSource dataSource) {
    this.context = context;
    this.dataSource = dataSource;
  }

  public JdbcConnectionTransactionResource addConnectionInitializer(
    final ConnectionConsumer initializer) {
    if (initializer != null) {
      try (
        var l = ThreadUtil.lock(this.lock)) {
        if (this.connection == null) {
          if (this.connectionInitializers == null) {
            this.connectionInitializers = new ArrayList<>();
          }
          this.connectionInitializers.add(initializer);
        } else {
          try {
            initializer.accept(this.connection);
          } catch (final SQLException e) {
            throw this.dataSource.getException("initialize", null, e);
          }
        }
      }
    }
    return this;
  }

  @Override
  public void afterCompletion() {
  }

  @Override
  public void beforeCompletion() {

  }

  @Override
  public void close() {
    try (
      var l = ThreadUtil.lock(this.lock)) {
      if (this.connection != null) {
        if (this.context.isReadOnly()) {
          try {
            this.connection.setReadOnly(false);
          } catch (final SQLException e) {
            Debug.noOp();
          }
        }
        try {
          this.connection.close();
        } catch (final SQLException e) {
          throw this.dataSource.getException("close", null, e);
        }
      }
    }
  }

  @Override
  public void commit() {
    try (
      var l = ThreadUtil.lock(this.lock)) {
      if (this.connection != null) {
        try {
          this.connection.commit();
        } catch (final SQLException e) {
          throw this.dataSource.getException("commit", null, e);
        }
      }
    }
  }

  private Connection getConnection() throws SQLException {
    try (
      var l = ThreadUtil.lock(this.lock)) {
      if (this.connection == null) {
        final Connection connection = this.dataSource.getConnectionInternal();
        try {
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
          if (this.connectionInitializers != null) {
            for (final ConnectionConsumer initializer : this.connectionInitializers) {
              initializer.accept(connection);
            }
          }
        } catch (SQLException | RuntimeException | Error e) {
          try {
            connection.close();
          } catch (final SQLException e2) {
          }
          throw e;
        }
        this.connection = connection;
      }
      return this.connection;
    }
  }

  public JdbcConnection newJdbcConnection() throws SQLException {
    final Connection connection = getConnection();
    if (connection == null) {
      return null;
    } else {
      return new JdbcConnection(this.dataSource, connection, false);
    }
  }

  @Override
  public void resume() {
  }

  @Override
  public void rollback() {
    try (
      var l = ThreadUtil.lock(this.lock)) {
      if (this.connection != null) {
        try {
          this.connection.rollback();
        } catch (final SQLException e) {
          throw this.dataSource.getException("rollback", null, e);
        }
      }
    }
  }

  @Override
  public void suspend() {

  }
}
