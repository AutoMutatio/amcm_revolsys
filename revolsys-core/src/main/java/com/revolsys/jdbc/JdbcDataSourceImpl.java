package com.revolsys.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.exception.DatabaseNotFoundException;
import com.revolsys.logging.Logs;
import com.revolsys.parallel.SemaphoreEx;
import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Property;
import com.revolsys.util.concurrent.Concurrent;

// TODO see why it is not cleaning up old connections.

public class JdbcDataSourceImpl extends JdbcDataSource implements BaseCloseable {
  public static class Builder {

    private final Properties connectionProperties = new Properties();

    private Driver driver;

    private String driverClassName;

    private Duration maxAge = Duration.ofHours(1);

    private int maxIdle = 2;

    private int maxPoolSize = 8;

    private Duration maxWait = Duration.ofSeconds(-1);

    private Supplier<String> passwordSupplier;

    private boolean rollbackOnReturn = true;

    private String url;

    private Supplier<String> userSupplier;

    public JdbcDataSourceImpl build() {
      return new JdbcDataSourceImpl(this);
    }

    public Builder setConfig(final MapEx newConfig) {
      for (final Entry<String, Object> property : newConfig.entrySet()) {
        final String name = property.getKey();
        final Object value = property.getValue();
        try {
          if (!Property.setSimple(this, name, value)) {
            this.connectionProperties.setProperty(name, value.toString());
          }
        } catch (final Throwable t) {
          Logs.debug(this,
            "Unable to set data source property " + name + " = " + value + " for " + this.url, t);
        }
      }
      return this;
    }

    public Builder setDriverClassName(final String driverClassName) {
      this.driverClassName = driverClassName;
      if (driverClassName != null) {
        try {
          Class<?> driverClass;
          try {
            driverClass = Class.forName(driverClassName);
          } catch (final ClassNotFoundException cnfe) {
            driverClass = Concurrent.contextClassLoader()
              .loadClass(driverClassName);
          }
          this.driver = (Driver)driverClass.getConstructor()
            .newInstance();
        } catch (final Exception t) {
          throw Exceptions.wrap("Cannot load JDBC driver class", t)
            .property("driverClassName", driverClassName);
        }
      }
      return this;
    }

    public Builder setMaxAge(final Duration maxAge) {
      this.maxAge = maxAge;
      return this;
    }

    public Builder setMaxIdle(final int maxIdle) {
      this.maxIdle = maxIdle;
      return this;
    }

    public Builder setMaxPoolSize(final int maxPoolSize) {
      if (maxPoolSize > 0) {
        this.maxPoolSize = maxPoolSize;
      } else {
        this.maxPoolSize = Integer.MAX_VALUE;
      }
      return this;
    }

    public Builder setMaxWait(final Duration maxWait) {
      this.maxWait = maxWait;
      return this;
    }

    public Builder setPasswordSupplier(final Supplier<String> passwordSupplier) {
      this.passwordSupplier = passwordSupplier;
      return this;
    }

    public Builder setRollbackOnReturn(final boolean rollbackOnReturn) {
      this.rollbackOnReturn = rollbackOnReturn;
      return this;
    }

    public Builder setUrl(final String url) {
      this.url = url;
      return this;
    }

    public Builder setUserSupplier(final Supplier<String> userSupplier) {
      this.userSupplier = userSupplier;
      return this;
    }
  }

  private class ConnectionEntry {
    private static final AtomicInteger INDEX = new AtomicInteger();

    private Connection connection;

    private final long expiryMillis = System.currentTimeMillis()
      + JdbcDataSourceImpl.this.maxAgeMillis;

    private final int index = INDEX.incrementAndGet();

    private Instant returnedInstant = Instant.EPOCH;

    private final AtomicReference<ConnectionEntryState> state = new AtomicReference<>(
      ConnectionEntryState.IDLE);

    private ConnectionEntry(final Connection connection) {
      this.connection = connection;
    }

    private Connection aquire() {
      final var connection = this.connection;
      if (this.state.compareAndSet(ConnectionEntryState.IDLE, ConnectionEntryState.ACQUIRED)) {
        return connection;
      } else {
        throw new IllegalStateException(
          "Cannot aquire connection as it is not idle: " + this.state);
      }
    }

    private void close() {
      if (this.state.compareAndSet(ConnectionEntryState.ACQUIRED, ConnectionEntryState.IDLE)) {
        decrementPoolSize();
      }
      if (this.state.compareAndSet(ConnectionEntryState.IDLE, ConnectionEntryState.CLOSED)) {
        final var connection = this.connection;
        this.connection = null;
        BaseCloseable.closeSilent(connection);
      }
    }

    public Connection getConnection() {
      return this.connection;
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(this.index);
    }

    public boolean isClosed() {
      try {
        if (this.state.get() == ConnectionEntryState.CLOSED) {
          return true;
        } else if (this.connection != null && this.connection.isClosed()) {
          return true;
        }
        return false;
      } catch (final SQLException e) {
        return true;
      }
    }

    public boolean isExpired() {
      final var time = System.currentTimeMillis();
      return isExpired(time);
    }

    public boolean isExpired(final long time) {
      if (isClosed()) {
        return true;
      } else if (time > this.expiryMillis) {
        close();
        return true;
      } else {
        return false;
      }
    }

    private void release(final boolean hasError) throws SQLException {
      if (this.state.compareAndSet(ConnectionEntryState.ACQUIRED, ConnectionEntryState.IDLE)) {
        @SuppressWarnings("resource")
        final var dataSource = JdbcDataSourceImpl.this;
        try {
          this.returnedInstant = Instant.now();
          final var connection = this.connection;
          if (connection != null) {
            final boolean connAutoCommit = connection.getAutoCommit();
            if (JdbcDataSourceImpl.this.config.rollbackOnReturn) {
              if (!connAutoCommit && !connection.isReadOnly()) {
                connection.rollback();
              }
            }
            connection.clearWarnings();
            if (!connAutoCommit) {
              connection.setAutoCommit(true);
            }
          }
        } catch (final SQLException | RuntimeException | Error e) {
          close();
          throw e;
        } finally {
          try {
            final var returnedMillis = this.returnedInstant.toEpochMilli();
            if (isExpired(returnedMillis)) {
            } else if (!canIdle()) {
              close();
            } else if (dataSource.isClosed()) {
              close();
            } else {
              dataSource.idleConnections.addLast(this);
            }
          } finally {
            decrementPoolSize();
          }
        }
      } else {
        final var state = this.state.get();
        if (state == ConnectionEntryState.IDLE) {
          throw new IllegalStateException(
            "Cannot release connection as it is not aqcuired:  " + state);
        }
      }
    }

    @Override
    public String toString() {
      return JdbcDataSourceImpl.this.toString() + "-" + this.index;
    }
  }

  private enum ConnectionEntryState {
    ACQUIRED, CLOSED, IDLE
  }

  private class ConnectionImpl extends JdbcConnection {

    private ConnectionEntry entry;

    public ConnectionImpl(final JdbcDataSource dataSource, final ConnectionEntry entry,
      final Connection connection, final boolean close) throws SQLException {
      super(dataSource, connection, close);
      this.entry = entry;
    }

    @Override
    protected void preClose() throws SQLException {
      this.entry.release(false);
      this.entry = null;
    }

  }

  private class ConnectionTransactionResource extends JdbcConnectionTransactionResource {
    private ConnectionEntry entry;

    private ConnectionTransactionResource(final TransactionContext context) {
      super(context);
    }

    @Override
    protected void closeConnection() throws SQLException {
      this.entry.release(isHasError());
      this.entry = null;
    }

    @Override
    protected JdbcDataSourceImpl getDataSource() {
      return JdbcDataSourceImpl.this;
    }

    @Override
    protected Connection newConnection() throws SQLException {
      final var entry = getConnectionEntry();
      try {
        final var connection = entry.getConnection();
        initializeConnection(connection);
        this.entry = entry;
        return connection;
      } catch (SQLException | RuntimeException | Error e) {
        entry.close();
        throw e;
      }
    }

    @Override
    protected JdbcConnection newJdbcConnection(final Connection connection) throws SQLException {
      return new ConnectionImpl(JdbcDataSourceImpl.this, this.entry, connection, false);
    }
  }

  private final AtomicBoolean closed = new AtomicBoolean();

  private final Builder config;

  private volatile ListEx<Consumer<Connection>> connectionInitCallbacks = ListEx.empty();

  private final ScheduledFuture<?> expiredFuture;

  private final ConcurrentLinkedDeque<ConnectionEntry> idleConnections = new ConcurrentLinkedDeque<>();

  private SemaphoreEx limitIdle;

  private final SemaphoreEx limitLeases;

  private PrintWriter logWriter;

  private final long maxAgeMillis;

  private JdbcDataSourceImpl(final Builder config) {
    this.config = config;
    this.maxAgeMillis = config.maxAge.toMillis();

    final String url = config.url;
    if (config.driver == null) {
      try {
        config.driver = DriverManager.getDriver(url);
      } catch (final SQLException e) {
        throw new DatabaseNotFoundException("Cannot create JDBC driver for URL", e);
      }
    }
    try {
      if (!config.driver.acceptsURL(url)) {
        throw new SQLException("No suitable driver", "08001");
      }
    } catch (final SQLException e) {
      throw new DatabaseNotFoundException("Cannot create JDBC driver for URL", e);
    }

    this.limitLeases = new SemaphoreEx(config.maxPoolSize);
    final var maxIdle = Math.min(config.maxIdle, config.maxPoolSize);
    if (maxIdle > 0) {
      this.limitIdle = new SemaphoreEx(maxIdle);
    }

    // Eagerly expire connections every minute
    this.expiredFuture = Concurrent.virtualSceduled()
      .scheduleWithFixedDelay(this::removeExpired, 1, 1, TimeUnit.MINUTES);
  }

  final void assertOpen() {
    if (isClosed()) {
      throw new IllegalStateException("Data Source closed");
    }
  }

  public boolean canIdle() {
    if (this.limitIdle == null || isClosed()) {
      return false;
    } else {
      return this.limitIdle.tryAcquire();
    }
  }

  @Override
  public void close() {
    if (!this.closed.compareAndSet(false, true)) {
      try {
        this.expiredFuture.cancel(true);
      } catch (final Exception e) {
      }
      while (!this.idleConnections.isEmpty()) {
        final var entry = nextEntry();
        if (entry != null) {
          entry.close();
        }
      }
      this.limitLeases.release(this.config.maxPoolSize - this.limitLeases.availablePermits());
    }
  }

  private void decrementPoolSize() {
    if (!isClosed()) {
      this.limitLeases.release();
    }
  }

  @Override
  public Connection getConnection(final String user, final String pass) {
    throw new UnsupportedOperationException("getConnection(user,password) is not supported");
  }

  public ListEx<Consumer<Connection>> getConnectionCallbacks() {
    return this.connectionInitCallbacks;
  }

  protected ConnectionEntry getConnectionEntry() {
    if (!isClosed() && this.limitLeases.tryAcquire(this.config.maxWait) && !isClosed()) {
      var entry = nextEntry();
      if (entry == null) {
        Connection connection = null;
        try {
          try {
            connection = newConnectionDo();
          } catch (final SQLException e) {
            throw getException("New connection", null, e);
          }
          if (connection == null) {
            throw new CannotGetJdbcConnectionException(
              "JDBC driver doesn't support connection URL");
          }
          for (final Consumer<Connection> callback : JdbcDataSourceImpl.this.connectionInitCallbacks) {
            callback.accept(connection);
          }
          entry = new ConnectionEntry(connection);
        } catch (RuntimeException | Error e) {
          BaseCloseable.closeSilent(connection);
          decrementPoolSize();
          throw e;
        }
      }
      entry.aquire();
      return entry;
    }
    if (isClosed()) {
      throw new IllegalStateException("Data source closed");
    } else {
      throw new TransientDataAccessResourceException("Pool size wait timeout");
    }

  }

  public String getDriverClassName() {
    return this.config.driverClassName;
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public PrintWriter getLogWriter() {
    return this.logWriter;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  public boolean isClosed() {
    return this.closed.get();
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface != null && iface.isInstance(this);
  }

  private Connection newConnectionDo() throws SQLException {
    final var url = this.config.url;
    final var userSupplier = this.config.userSupplier;
    final var passwordSupplier = this.config.passwordSupplier;
    final var connectionProperties = new Properties(this.config.connectionProperties);

    if (userSupplier != null) {
      final var user = userSupplier.get();
      if (user == null) {
        throw new IllegalArgumentException("User was null");
      }
      connectionProperties.setProperty("user", user);
    }

    if (passwordSupplier != null) {
      final var password = passwordSupplier.get();
      if (password == null) {
        throw new IllegalArgumentException("password was null");
      }
      connectionProperties.setProperty("password", password);
    }

    return this.config.driver.connect(url, connectionProperties);
  }

  @Override
  protected JdbcConnectionTransactionResource newConnectionTransactionResource(
    final ActiveTransactionContext context) {
    return new ConnectionTransactionResource(context);
  }

  @Override
  protected SQLErrorCodeSQLExceptionTranslator newExceptionTranslator() {
    final var translator = new SQLErrorCodeSQLExceptionTranslator();
    try {
      try (
        var connection = newConnectionDo()) {
        final var metadata = connection.getMetaData();
        final var productName = metadata.getDatabaseProductName();
        final var errorCodes = SQLErrorCodesFactory.getInstance()
          .getErrorCodes(productName);
        translator.setSqlErrorCodes(errorCodes);
      }
    } catch (final SQLException e) {
      return translator;
    } catch (final Throwable e) {
      if (Exceptions.isInterruptException(e)) {
        throw e;
      }
    }
    return translator;
  }

  @Override
  protected JdbcConnection newJdbcConnection() throws SQLException {
    final var entry = getConnectionEntry();
    final var connection = entry.getConnection();
    return new ConnectionImpl(this, entry, connection, true);
  }

  private ConnectionEntry nextEntry() {
    while (true) {
      final ConnectionEntry entry = this.idleConnections.pollFirst();
      if (entry == null) {
        return null;
      } else {
        this.limitIdle.release();
        if (!entry.isExpired()) {
          return entry;
        }
      }
    }
  }

  private void removeExpired() {
    this.idleConnections.removeIf(ConnectionEntry::isExpired);
  }

  public JdbcDataSourceImpl setConnectionInitCallbacks(
    final ListEx<Consumer<Connection>> connectionInitCallbacks) {
    this.connectionInitCallbacks = connectionInitCallbacks;
    return this;
  }

  @Override
  public void setLoginTimeout(final int loginTimeout) throws SQLException {
    throw new UnsupportedOperationException("Not supported by JdbcDataSourceImpl");
  }

  @Override
  public void setLogWriter(final PrintWriter logWriter) {
    this.logWriter = logWriter;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException(this + " is not a wrapper for " + iface);
  }
}
