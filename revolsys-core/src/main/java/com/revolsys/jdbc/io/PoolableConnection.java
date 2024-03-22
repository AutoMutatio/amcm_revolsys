/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.revolsys.jdbc.io;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.dbcp2.DelegatingConnection;
import org.apache.commons.dbcp2.PoolingConnection;
import org.apache.commons.pool2.ObjectPool;

import com.revolsys.exception.Exceptions;
import com.revolsys.util.BaseCloseable;

/**
 * A delegating connection that, rather than closing the underlying connection,
 * returns itself to an {@link ObjectPool}
 * when closed.
 *
 * @since 2.0
 */
public class PoolableConnection extends DelegatingConnection<Connection> {
  private final JdbcDataSourceImpl dataSource;
  // Use a prepared statement for validation, retaining the last used SQL to
  // check if the validation query has changed.
  private PreparedStatement validationPreparedStatement;

  private String lastValidationSql;
  /**
   * Indicate that unrecoverable SQLException was thrown when using this
   * connection. Such a connection should be
   * considered broken and not pass validation in the future.
   */
  private boolean fatalSqlExceptionThrown;

  private final Lock lock = new ReentrantLock();

  public PoolableConnection(final Connection conn, final JdbcDataSourceImpl dataSource) {
    super(conn);
    this.dataSource = dataSource;
  }

  @Override
  protected void activate() {
    super.activate();
  }

  /**
   * Returns me to my pool.
   */
  @Override
  public void close() throws SQLException {
    this.lock.lock();
    try {
      if (isClosedInternal()) {
        // already closed
        return;
      }

      boolean isUnderlyingConnectionClosed;
      try {
        isUnderlyingConnectionClosed = getDelegateInternal().isClosed();
      } catch (final SQLException e) {
        try {
          this.dataSource.invalidateObject(this);
        } catch (final IllegalStateException ise) {
          // pool is closed, so close the connection
          passivate();
          getInnermostDelegate().close();
        } catch (final Exception ignored) {
          // DO NOTHING the original exception will be rethrown
        }
        throw new SQLException("Cannot close connection (isClosed check failed)", e);
      }

      /*
       * Can't set close before this code block since the connection needs to be open
       * when validation runs. Can't set
       * close after this code block since by then the connection will have been
       * returned to the pool and may have
       * been borrowed by another thread. Therefore, the close flag is set in
       * passivate().
       */
      if (isUnderlyingConnectionClosed) {
        // Abnormal close: underlying connection closed unexpectedly, so we
        // must destroy this proxy
        try {
          this.dataSource.invalidateObject(this);
        } catch (final IllegalStateException e) {
          // pool is closed, so close the connection
          passivate();
          getInnermostDelegate().close();
        } catch (final Exception e) {
          throw new SQLException("Cannot close connection (invalidating pooled object failed)", e);
        }
      } else {
        // Normal close: underlying connection is still open, so we
        // simply need to return this proxy to the pool
        try {
          this.dataSource.returnObject(this);
        } catch (final IllegalStateException e) {
          // pool is closed, so close the connection
          passivate();
          getInnermostDelegate().close();
        } catch (final Error | RuntimeException e) {
          throw e;
        } catch (final Exception e) {
          throw new SQLException("Cannot close connection (return to pool failed)", e);
        }
      }
    } finally {
      this.lock.unlock();
    }
  }

  @Override
  protected void handleException(final SQLException e) throws SQLException {
    this.fatalSqlExceptionThrown |= isFatalException(e);
    super.handleException(e);
  }

  /**
   * {@inheritDoc}
   * <p>
   * This method should not be used by a client to determine whether or not a
   * connection should be return to the
   * connection pool (by calling {@link #close()}). Clients should always attempt
   * to return a connection to the pool
   * once it is no longer required.
   */
  @Override
  public boolean isClosed() throws SQLException {
    if (isClosedInternal()) {
      return true;
    }

    if (getDelegateInternal().isClosed()) {
      // Something has gone wrong. The underlying connection has been
      // closed without the connection being returned to the pool. Return
      // it now.
      close();
      return true;
    }

    return false;
  }

  boolean isDisconnectionSqlException(final SQLException e) {
    boolean fatalException = false;
    final String sqlState = e.getSQLState();
    if (sqlState != null) {
      fatalException = sqlState.startsWith("08");
    }
    return fatalException;
  }

  /**
   * Checks the SQLState of the input exception and any nested SQLExceptions it
   * wraps.
   * <p>
   * If {@link #disconnectionSqlCodes} has been set, sql states are compared to
   * those in the
   * configured list of fatal exception codes. If this property is not set, codes
   * are compared against the default
   * codes in {@link Utils#getDisconnectionSqlCodes()} and in this case anything
   * starting with #{link
   * Utils.DISCONNECTION_SQL_CODE_PREFIX} is considered a disconnection.
   * </p>
   *
   * @param e
   *          SQLException to be examined
   * @return true if the exception signals a disconnection
   */
  boolean isFatalException(final SQLException e) {
    boolean fatalException = isDisconnectionSqlException(e);
    if (!fatalException) {
      SQLException parentException = e;
      SQLException nextException = e.getNextException();
      while (nextException != null && nextException != parentException && !fatalException) {
        fatalException = isDisconnectionSqlException(nextException);
        parentException = nextException;
        nextException = parentException.getNextException();
      }
    }
    return fatalException;
  }

  @Override
  protected void passivate() throws SQLException {
    super.passivate();
    setClosedInternal(true);
    if (getDelegateInternal() instanceof PoolingConnection) {
      ((PoolingConnection) getDelegateInternal()).connectionReturnedToPool();
    }
  }

  /**
   * Actually close my underlying {@link Connection}.
   */
  public void reallyClose() {

    if (this.validationPreparedStatement != null) {
      BaseCloseable.closeSilent(this.validationPreparedStatement);
    }

    try {
      super.closeInternal();
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  /**
   * Validates the connection, using the following algorithm:
   * <ol>
   * <li>If {@code fastFailValidation} (constructor argument) is {@code true} and
   * this connection has previously
   * thrown a fatal disconnection exception, a {@code SQLException} is
   * thrown.</li>
   * <li>If {@code sql} is null, the driver's #{@link Connection#isValid(int)
   * isValid(timeout)} is called. If it
   * returns {@code false}, {@code SQLException} is thrown; otherwise, this method
   * returns successfully.</li>
   * <li>If {@code sql} is not null, it is executed as a query and if the
   * resulting {@code ResultSet} contains at
   * least one row, this method returns successfully. If not, {@code SQLException}
   * is thrown.</li>
   * </ol>
   *
   * @param sql
   *                        The validation SQL query.
   * @param timeoutDuration
   *                        The validation timeout in seconds.
   * @throws SQLException
   *                      Thrown when validation fails or an SQLException occurs
   *                      during validation
   * @since 2.10.0
   */
  public void validate(final String sql, Duration timeoutDuration) throws SQLException {
    if (this.fatalSqlExceptionThrown) {
      throw new SQLException("Connection failed witha a fatal exception");
    }

    if (sql == null || sql.isEmpty()) {
      if (timeoutDuration.isNegative()) {
        timeoutDuration = Duration.ZERO;
      }
      if (!isValid(timeoutDuration)) {
        throw new SQLException("isValid() returned false");
      }
      return;
    }

    if (!sql.equals(this.lastValidationSql)) {
      this.lastValidationSql = sql;
      // Has to be the innermost delegate else the prepared statement will
      // be closed when the pooled connection is passivated.
      this.validationPreparedStatement = getInnermostDelegateInternal().prepareStatement(sql);
    }

    if (timeoutDuration.compareTo(Duration.ZERO) > 0) {
      this.validationPreparedStatement.setQueryTimeout((int) timeoutDuration.getSeconds());
    }

    try (ResultSet rs = this.validationPreparedStatement.executeQuery()) {
      if (!rs.next()) {
        throw new SQLException("validationQuery didn't return a row");
      }
    } catch (final SQLException sqle) {
      throw sqle;
    }
  }

  /**
   * Validates the connection, using the following algorithm:
   * <ol>
   * <li>If {@code fastFailValidation} (constructor argument) is {@code true} and
   * this connection has previously
   * thrown a fatal disconnection exception, a {@code SQLException} is
   * thrown.</li>
   * <li>If {@code sql} is null, the driver's #{@link Connection#isValid(int)
   * isValid(timeout)} is called. If it
   * returns {@code false}, {@code SQLException} is thrown; otherwise, this method
   * returns successfully.</li>
   * <li>If {@code sql} is not null, it is executed as a query and if the
   * resulting {@code ResultSet} contains at
   * least one row, this method returns successfully. If not, {@code SQLException}
   * is thrown.</li>
   * </ol>
   *
   * @param sql
   *                       The validation SQL query.
   * @param timeoutSeconds
   *                       The validation timeout in seconds.
   * @throws SQLException
   *                      Thrown when validation fails or an SQLException occurs
   *                      during validation
   * @deprecated Use {@link #validate(String, Duration)}.
   */
  @Deprecated
  public void validate(final String sql, final int timeoutSeconds) throws SQLException {
    validate(sql, Duration.ofSeconds(timeoutSeconds));
  }
}
