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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.jdbc.support.SQLErrorCodesFactory;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.logging.Logs;
import com.revolsys.parallel.ExecutorServiceFactory;
import com.revolsys.parallel.SemaphoreEx;
import com.revolsys.transaction.ActiveTransactionContext;
import com.revolsys.transaction.TransactionContext;
import com.revolsys.util.BaseCloseable;
import com.revolsys.util.Property;

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
        final var state = this.state.get();
        if (state == ConnectionEntryState.ACQUIRED) {
          throw new IllegalStateException("Cannot aquire connection as it is not idle:  " + state);
        }
      }
      return null;
    }

    private void close() {
      if (this.state.compareAndSet(ConnectionEntryState.ACQUIRED, ConnectionEntryState.IDLE)) {
        JdbcDataSourceImpl.this.poolSizeSemaphore.release();
      }
      final var oldState = this.state.get();
      if (oldState != ConnectionEntryState.CLOSED
        && this.state.compareAndSet(oldState, ConnectionEntryState.CLOSED)) {
        final var connection = this.connection;
        this.connection = null;
        if (JdbcDataSourceImpl.this.idleConnections.remove(this)) {
          JdbcDataSourceImpl.this.idleCount.decrementAndGet();
        }
        BaseCloseable.closeSilent(connection);
      }
    }

    public Duration getIdleDuration() {
      return Duration.between(this.returnedInstant, Instant.now());
    }

    @Override
    public int hashCode() {
      return Integer.hashCode(this.index);
    }

    public boolean isClosed() {
      return this.state.get() == ConnectionEntryState.CLOSED;
    }

    public boolean isExpired(final long time) {
      if (time > this.expiryMillis) {
        return true;
      } else {
        return false;
      }
    }

    private void release() throws SQLException {
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
          final int maxIdle = getMaxIdle();
          if (maxIdle > -1 && maxIdle <= dataSource.idleCount.get()
            || isExpired(this.returnedInstant.toEpochMilli())) {
            close();
          } else if (dataSource.isClosed()) {
            close();
          } else {
            dataSource.idleCount.incrementAndGet();
            dataSource.idleConnections.addLast(this);
            dataSource.poolSizeSemaphore.release();
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
    IDLE, ACQUIRED, CLOSED
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
      this.entry.release();
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
      this.entry.release();
      if (isHasError()) {
        removeEntry(this.entry);
      }
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
        final var connection = entry.aquire();
        if (connection == null) {
          throw new CannotGetJdbcConnectionException("Connection was null");
        }
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
    NEW, INITIALIZING, INITIALIZED, CLOSED
  }

  private static final Duration MAX_DURATION = Duration.ofMillis(Long.MAX_VALUE);

  private final AtomicBoolean closed = new AtomicBoolean();

  private volatile ListEx<Consumer<Connection>> connectionInitCallbacks = ListEx.empty();

  private String url;

  private Duration defaultQueryTimeoutDuration;

  private Driver driver;

  private ClassLoader driverClassLoader;

  private String driverClassName;

  private boolean rollbackOnReturn = true;

  private int minPoolSize = 0;

  private int maxPoolSize = 8;

  private Duration maxWait = Duration.ofSeconds(-1);

  private final ConcurrentLinkedDeque<ConnectionEntry> idleConnections = new ConcurrentLinkedDeque<>();

  private final int numTestsPerEvictionRun = 10;

  private PrintWriter logWriter;

  private final Duration idleEvictDuration = MAX_DURATION;

  private final Duration idleSoftEvictDuration = MAX_DURATION;

  private int minIdle = 0;

  private int maxIdle = 2;

  private Duration minEvictableIdle;

  private Duration maxAge = Duration.ofHours(1);

  private long maxAgeMillis = this.maxAge.toMillis();

  private int evictDelaySeconds;

  private Future<?> evictor;

  private SemaphoreEx poolSizeSemaphore;

  private final AtomicReference<DataSourceStateState> state = new AtomicReference<>(
    DataSourceStateState.NEW);

  private final CountDownLatch initLatch = new CountDownLatch(1);

  private final AtomicInteger idleCount = new AtomicInteger();

  private Supplier<String> userSupplier;

  private Supplier<String> passwordSupplier;

  private final Properties connectionProperties = new Properties();

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

  public void clear() {
    while (!this.idleConnections.isEmpty()) {
      final var entry = nextEntry();
      if (entry != null) {
        entry.close();
      }
    }
  }

  @Override
  public void close() {
    if (!this.closed.compareAndSet(false, true)) {
      clear();
      this.poolSizeSemaphore.release(this.poolSizeSemaphore.availablePermits());
      final var evictor = this.evictor;
      if (evictor != null) {
        evictor.cancel(true);
      }
    }
  }

  private void evict() {
    final long time = System.currentTimeMillis();
    int count = 0;
    final var iterator = this.idleConnections.iterator();
    while (iterator.hasNext() && count++ < this.numTestsPerEvictionRun && !isClosed()) {
      final var entry = iterator.next();
      if (entry.isExpired(time)) {
        removeEntry(entry);
      } else {
        final int idleCount = this.idleCount.get();
        final Duration idleDuration = entry.getIdleDuration();
        final var idleSoftEvict = getIdleSoftEvictDuration();
        final var idleEvict = getIdleEvictDuration();
        if (idleSoftEvict.compareTo(idleDuration) < 0 && getMinIdle() < idleCount
          || idleEvict.compareTo(idleDuration) < 0) {
          removeEntry(entry);
        } else if (entry.isClosed()) {
          removeEntry(entry);
        }
      }
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
    if (!isClosed()) {
      if (this.poolSizeSemaphore.tryAcquire(this.maxWait)) {
        var entry = nextEntry();
        if (entry == null) {
          try {
            final var connection = newConnectionDo();
            if (connection == null) {
              throw new CannotGetJdbcConnectionException(
                "JDBC driver doesn't support connection URL");
            }
            try {
              for (final Consumer<Connection> callback : JdbcDataSourceImpl.this.connectionInitCallbacks) {
                callback.accept(connection);
              }
            } catch (final RuntimeException | Error e) {
              BaseCloseable.closeSilent(connection);
              throw e;
            }
            entry = new ConnectionEntry(connection);
          } catch (final DataAccessException e) {
            this.poolSizeSemaphore.release();
            throw e;
          } catch (final SQLException e) {
            this.poolSizeSemaphore.release();
            throw getException("New connection", null, e);
          } catch (RuntimeException | Error e) {
            this.poolSizeSemaphore.release();
            throw e;
          }
        }
        if (isClosed()) {
          entry.close();
        } else {
          return entry;
        }
      } else if (!isClosed()) {
        throw new TransientDataAccessResourceException("Pool size wait timeout");
      }
    }
    throw new IllegalStateException("Data source closed");

  }

  public Duration getDefaultQueryTimeoutDuration() {
    return this.defaultQueryTimeoutDuration;
  }

  public Driver getDriver() {
    return this.driver;
  }

  public ClassLoader getDriverClassLoader() {
    return this.driverClassLoader;
  }

  public String getDriverClassName() {
    return this.driverClassName;
  }

  public Duration getIdleEvictDuration() {
    return this.idleEvictDuration;
  }

  public Duration getIdleSoftEvictDuration() {
    return this.idleSoftEvictDuration;
  }

  @Override
  public int getLoginTimeout() {
    return 0;
  }

  @Override
  public PrintWriter getLogWriter() {
    return this.logWriter;
  }

  public int getMaxIdle() {
    return this.maxIdle;
  }

  public int getMaxPoolSize() {
    return this.maxPoolSize;
  }

  public Duration getMaxWait() {
    return this.maxWait;
  }

  public Duration getMinEvictableIdle() {
    return this.minEvictableIdle;
  }

  public int getMinIdle() {
    return this.minIdle;
  }

  public int getMinPoolSize() {
    return this.minPoolSize;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  public String getUrl() {
    return this.url;
  }

  private void initialize() {
    try {
      if (this.driver == null) {
        final String driverClassName = getDriverClassName();
        final ClassLoader driverClassLoader = getDriverClassLoader();
        final String url = getUrl();
        Class<?> driverFromCCL = null;
        if (driverClassName != null) {
          try {
            try {
              if (driverClassLoader == null) {
                driverFromCCL = Class.forName(driverClassName);
              } else {
                driverFromCCL = Class.forName(driverClassName, true, driverClassLoader);
              }
            } catch (final ClassNotFoundException cnfe) {
              driverFromCCL = Thread.currentThread()
                .getContextClassLoader()
                .loadClass(driverClassName);
            }
          } catch (final Exception t) {
            final String message = "Cannot load JDBC driver class '" + driverClassName + "'";
            throw new SQLException(message, t);
          }
        }

        try {
          if (driverFromCCL == null) {
            this.driver = DriverManager.getDriver(url);
          } else {
            // Usage of DriverManager is not possible, as it does not
            // respect the ContextClassLoader
            // N.B. This cast may cause ClassCastException which is
            // handled below
            this.driver = (Driver)driverFromCCL.getConstructor()
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
    this.poolSizeSemaphore = new SemaphoreEx(this.maxPoolSize);
    this.evictor = ExecutorServiceFactory.getScheduledVirtual()
      .scheduleWithFixedDelay(this::evict, this.evictDelaySeconds, this.evictDelaySeconds,
        TimeUnit.SECONDS);
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
    final var url = getUrl();
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
    final var connection = entry.aquire();
    if (connection == null) {
      throw new CannotGetJdbcConnectionException("Connection was null");
    }
    return new ConnectionImpl(this, entry, connection, true);
  }

  private ConnectionEntry nextEntry() {
    final long time = System.currentTimeMillis();
    while (true) {
      final ConnectionEntry entry = this.idleConnections.pollFirst();
      if (entry == null) {
        return null;
      } else if (entry.isExpired(time)) {
        removeEntry(entry);
      } else {
        this.idleCount.decrementAndGet();
        return entry;
      }
    }
  }

  private void removeEntry(final ConnectionEntry entry) {
    entry.close();
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

  public JdbcDataSourceImpl setDefaultQueryTimeout(final Duration defaultQueryTimeoutDuration) {
    this.defaultQueryTimeoutDuration = defaultQueryTimeoutDuration;
    return this;
  }

  public JdbcDataSourceImpl setDriver(final Driver driver) {
    this.driver = driver;
    return this;
  }

  public JdbcDataSourceImpl setDriverClassLoader(final ClassLoader driverClassLoader) {
    this.driverClassLoader = driverClassLoader;
    return this;
  }

  public JdbcDataSourceImpl setDriverClassName(final String driverClassName) {
    this.driverClassName = driverClassName;
    return this;
  }

  public JdbcDataSourceImpl setDurationBetweenEvictionRuns(final Duration duration) {
    this.evictDelaySeconds = Math.max(1, (int)duration.toSeconds());
    return this;
  }

  @Override
  public void setLoginTimeout(final int loginTimeout) throws SQLException {
    // This method isn't supported by the PoolingDataSource returned by the
    // createDataSource
    throw new UnsupportedOperationException("Not supported by BasicDataSource");
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

  public final JdbcDataSourceImpl setMinEvictableIdle(final Duration minEvictableIdle) {
    this.minEvictableIdle = minEvictableIdle;
    return this;
  }

  public JdbcDataSourceImpl setMinIdle(final int minIdle) {
    this.minIdle = minIdle;
    return this;
  }

  public JdbcDataSourceImpl setMinPoolSize(final int minPoolSize) {
    this.minPoolSize = minPoolSize;
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
