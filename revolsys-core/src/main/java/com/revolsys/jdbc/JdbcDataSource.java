package com.revolsys.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import com.revolsys.collection.value.ValueHolder;
import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.transaction.Transactionable;

public abstract class JdbcDataSource implements DataSource, Transactionable {

  public static interface ConnectionConsumer {
    static ConnectionConsumer EMPTY = t -> {
    };

    void accept(Connection connection) throws SQLException;

    default ConnectionConsumer andThen(final ConnectionConsumer after) {
      if (after == null) {
        return this;
      } else {
        return v -> {
          accept(v);
          after.accept(v);
        };
      }
    }
  }

  public SQLErrorCodeSQLExceptionTranslator DEFAULT_ERROR_HANDLER = new SQLErrorCodeSQLExceptionTranslator();

  private final Object key = new Object();

  protected final ValueHolder<SQLErrorCodeSQLExceptionTranslator> exceptionTranslator = ValueHolder
    .lazy(this::newExceptionTranslator);

  private final Function<ActiveTransactionContext, JdbcConnectionTransactionResource> resourceConstructor = this::newConnectionTransactionResource;

  public JdbcDataSource() {
  }

  public void addConnectionInitializer(final ActiveTransactionContext activeContext,
    final ConnectionConsumer connection) {
    activeContext.getResource(this.key, this.resourceConstructor)
      .addConnectionInitializer(connection);
  }

  @Override
  public JdbcConnection getConnection() throws SQLException {
    return getConnection(null);
  }

  /**
   * Return a new JDBC connection. The client caller must close the connection.
   */
  public JdbcConnection getConnection(final ConnectionConsumer initializer) throws SQLException {
    final TransactionContext context = Transaction.getContext();
    if (context instanceof final ActiveTransactionContext activeContext) {
      return activeContext.getResource(this.key, this.resourceConstructor)
        .addConnectionInitializer(initializer)
        .getJdbcConnection();
    } else {
      final JdbcConnection connection = newJdbcConnection();
      if (connection.hasConnection()) {
        if (initializer != null) {
          initializer.accept(connection);
        }
      } else {
        throw new SQLException("No connection");
      }
      return connection;
    }
  }

  @Override
  public Connection getConnection(final String username, final String password)
    throws SQLException {
    throw new UnsupportedOperationException("Username/password connections are not supported");
  }

  public DataAccessException getException(final String task, final String sql,
    final SQLException e) {
    final var translatedException = this.exceptionTranslator.getValue()
      .translate(task, sql, e);
    if (translatedException == null) {
      return new UncategorizedSQLException(task, sql, e);
    } else {
      return translatedException;
    }
  }

  protected abstract JdbcConnectionTransactionResource newConnectionTransactionResource(
    final ActiveTransactionContext context);

  protected abstract SQLErrorCodeSQLExceptionTranslator newExceptionTranslator();

  protected abstract JdbcConnection newJdbcConnection() throws SQLException;
}
