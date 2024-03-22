package com.revolsys.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.UncategorizedSQLException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.Transaction;
import com.revolsys.transaction.TransactionContext;

public abstract class JdbcDataSource implements DataSource {

  public static interface ConnectionConsumer {
    void accept(Connection connection) throws SQLException;
  }

  private final Object key = new Object();

  protected final SQLErrorCodeSQLExceptionTranslator exceptionTranslator = new SQLErrorCodeSQLExceptionTranslator();

  private final Function<ActiveTransactionContext, JdbcConnectionTransactionResource> resourceConstructor = context -> new JdbcConnectionTransactionResource(
      context, this);

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
          .newJdbcConnection();
    } else {
      final Connection connection = getConnectionInternal();
      if (connection == null) {
        return null;
      } else {
        if (initializer != null) {
          initializer.accept(connection);
        }
        return new JdbcConnection(this, connection, true);
      }
    }
  }

  @Override
  public Connection getConnection(final String username, final String password)
      throws SQLException {
    throw new UnsupportedOperationException("Username/password connections are not supported");
  }

  protected abstract Connection getConnectionInternal() throws SQLException;

  public DataAccessException getException(final String task, final String sql,
      final SQLException e) {
    final DataAccessException translatedException = this.exceptionTranslator.translate(task, sql,
        e);
    if (translatedException == null) {
      return new UncategorizedSQLException(task, sql, e);
    } else {
      return translatedException;
    }
  }

  void releaseConnection(final Connection connection) throws SQLException {
    connection.close();
  }

}
