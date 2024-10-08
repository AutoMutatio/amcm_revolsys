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
import java.util.concurrent.CountDownLatch;
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
import com.revolsys.logging.Logs;
import com.revolsys.parallel.SemaphoreEx;
import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Property;
import com.revolsys.util.concurrent.Concurrent;

public class JdbcDataSourceImpl extends JdbcDataSource implements BaseCloseable {
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
        return this.state.get() == ConnectionEntryState.CLOSED
          || this.connection != null && !this.connection.isClosed();
      } catch (final SQLException e) {
        return true;
      }
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
            if (isRollbackOnReturn()) {
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
            if (isExpired(this.returnedInstant.toEpochMilli()) || !canIdle()) {
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

  private enum DataSourceStateState {
    CLOSED, INITIALIZED, INITIALIZING, NEW
  }

  private final AtomicBoolean closed = new AtomicBoolean();

  private volatile ListEx<Consumer<Connection>> connectionInitCallbacks = ListEx.empty();

  private final Properties connectionProperties = new Properties();

  private Driver driver;

  private String driverClassName;

  private final ConcurrentLinkedDeque<ConnectionEntry> idleConnections = new ConcurrentLinkedDeque<>();

  private final CountDownLatch initLatch = new CountDownLatch(1);

  private SemaphoreEx limitIdle;

  private SemaphoreEx limitLeases;

  private PrintWriter logWriter;

  private Duration maxAge = Duration.ofHours(1);

  private long maxAgeMillis = this.maxAge.toMillis();

  private int maxIdle = 2;

  private int maxPoolSize = 8;

  private Duration maxWait = Duration.ofSeconds(-1);

  private Supplier<String> passwordSupplier;

  private boolean rollbackOnReturn = true;

  private final AtomicReference<DataSourceStateState> state = new AtomicReference<>(
    DataSourceStateState.NEW);

  private String url;

  private Supplier<String> userSupplier;

  public JdbcDataSourceImpl() {
  }

  private void assertNotInitialized() {
    if (this.state.get() != DataSourceStateState.NEW) {
      throw new IllegalStateException("Cannot change settings when data source is initialized");
    }
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
      while (!this.idleConnections.isEmpty()) {
        final var entry = nextEntry();
        if (entry != null) {
          entry.close();
        }
      }
      this.limitLeases.release(this.maxPoolSize - this.limitLeases.availablePermits());
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
    if (this.state.compareAndSet(DataSourceStateState.NEW, DataSourceStateState.INITIALIZING)) {
      initialize();
    } else if (this.state.get() == DataSourceStateState.INITIALIZING) {
      try {
        this.initLatch.await();
      } catch (final InterruptedException e) {
        throw Exceptions.toRuntimeException(e);
      }
    }
    if (!isClosed() && this.limitLeases.tryAcquire(this.maxWait) && !isClosed()) {
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
    return this.driverClassName;
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

  private void initialize() {
    try {
      if (this.driver == null) {
        final String driverClassName = getDriverClassName();
        final String url = this.url;
        Class<?> driver = null;
        if (driverClassName != null) {
          try {
            try {
              driver = Class.forName(driverClassName);
            } catch (final ClassNotFoundException cnfe) {
              driver = Concurrent.contextClassLoader()
                .loadClass(driverClassName);
            }
          } catch (final Exception t) {
            final String message = "Cannot load JDBC driver class '" + driverClassName + "'";
            throw new SQLException(message, t);
          }
        }

        try {
          if (driver == null) {
            this.driver = DriverManager.getDriver(url);
          } else {
            this.driver = (Driver)driver.getConstructor()
              .newInstance();
            if (!this.driver.acceptsURL(url)) {
              throw new SQLException("No suitable driver", "08001");
            }
          }
        } catch (final Exception t) {
          final String message = "Cannot create JDBC driver of class '"
            + (driverClassName != null ? driverClassName : "") + "' for connect URL '" + url + "'";
          throw new SQLException(message, t);
        }
      }
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
    this.limitLeases = new SemaphoreEx(this.maxPoolSize);
    final var maxIdle = Math.min(this.maxIdle, this.maxPoolSize);
    if (maxIdle > 0) {
      this.limitIdle = new SemaphoreEx(maxIdle);
    }
    if (this.state.compareAndSet(DataSourceStateState.INITIALIZING,
      DataSourceStateState.INITIALIZED)) {
      this.initLatch.countDown();
    }
  }

  public boolean isClosed() {
    return this.closed.get();
  }

  public boolean isRollbackOnReturn() {
    return this.rollbackOnReturn;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface != null && iface.isInstance(this);
  }

  private Connection newConnectionDo() throws SQLException {
    final var url = this.url;
    final var connectionProperties = new Properties(this.connectionProperties);
    if (this.userSupplier != null) {
      final var user = this.userSupplier.get();
      if (user == null) {
        throw new IllegalArgumentException("User was null");
      }
      connectionProperties.setProperty("user", user);
    }
    if (this.passwordSupplier != null) {
      final var password = this.passwordSupplier.get();
      if (password == null) {
        throw new IllegalArgumentException("password was null");
      }
      connectionProperties.setProperty("password", password);
    }
    final var connection = this.driver.connect(url, connectionProperties);
    return connection;
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
    final long time = System.currentTimeMillis();
    while (true) {
      final ConnectionEntry entry = this.idleConnections.pollFirst();
      if (entry == null) {
        return null;
      } else {
        this.limitIdle.release();
        if (!entry.isExpired(time)) {
          return entry;
        }
      }
    }
  }

  public JdbcDataSourceImpl setConfig(final MapEx newConfig) {
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

  public JdbcDataSourceImpl setConnectionInitCallbacks(
    final ListEx<Consumer<Connection>> connectionInitCallbacks) {
    this.connectionInitCallbacks = connectionInitCallbacks;
    return this;
  }

  public JdbcDataSourceImpl setDriverClassName(final String driverClassName) {
    this.driverClassName = driverClassName;
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

  public JdbcDataSourceImpl setMaxAge(final Duration maxAge) {
    this.maxAge = maxAge;
    this.maxAgeMillis = maxAge.toMillis();
    return this;
  }

  public JdbcDataSourceImpl setMaxIdle(final int maxIdle) {
    this.maxIdle = maxIdle;
    return this;
  }

  public JdbcDataSourceImpl setMaxPoolSize(final int maxPoolSize) {
    assertNotInitialized();
    if (maxPoolSize > 0) {
      this.maxPoolSize = maxPoolSize;
    } else {
      this.maxPoolSize = Integer.MAX_VALUE;
    }
    return this;
  }

  public JdbcDataSourceImpl setMaxWait(final Duration maxWait) {
    this.maxWait = maxWait;
    return this;
  }

  public JdbcDataSourceImpl setPasswordSupplier(final Supplier<String> passwordSupplier) {
    this.passwordSupplier = passwordSupplier;
    return this;
  }

  public JdbcDataSourceImpl setRollbackOnReturn(final boolean rollbackOnReturn) {
    this.rollbackOnReturn = rollbackOnReturn;
    return this;
  }

  public JdbcDataSourceImpl setUrl(final String url) {
    this.url = url;
    return this;
  }

  public JdbcDataSourceImpl setUserSupplier(final Supplier<String> userSupplier) {
    this.userSupplier = userSupplier;
    return this;
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException(this + " is not a wrapper for " + iface);
  }
}
