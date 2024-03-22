package com.revolsys.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.revolsys.jdbc.JdbcDataSource.ConnectionConsumer;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.transaction.Isolation;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.transaction.TransactionableResource;
import com.revolsys.util.Debug;

public class JdbcConnectionTransactionResource implements TransactionableResource {
  private final ReentrantLockEx lock = new ReentrantLockEx();

  private Connection connection;

  private final JdbcDataSource dataSource;

  private final TransactionContext context;

  private List<ConnectionConsumer> connectionInitializers;

  private final boolean closed = false;

  public JdbcConnectionTransactionResource(final TransactionContext context,
      final JdbcDataSource dataSource) {
    this.context = context;
    this.dataSource = dataSource;
  }

  public JdbcConnectionTransactionResource addConnectionInitializer(
      final ConnectionConsumer initializer) {
    if (initializer != null) {
      try (
          var l = this.lock.lockX()) {
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
  public void close() {
    try (
        var l = this.lock.lockX()) {
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
        var l = this.lock.lockX()) {
      if (this.connection != null) {
        try {
          this.connection.commit();
        } catch (final SQLException e) {
          throw this.dataSource.getException("commit", null, e);
        }
      }
    }
  }

  public JdbcConnection newJdbcConnection() throws SQLException {
    try (
        var l = this.lock.lockX()) {
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
      return new JdbcConnection(this.dataSource, this.connection, false);
    }
  }

  @Override
  public void resume() {
  }

  @Override
  public void rollback() {
    try (
        var l = this.lock.lockX()) {
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
