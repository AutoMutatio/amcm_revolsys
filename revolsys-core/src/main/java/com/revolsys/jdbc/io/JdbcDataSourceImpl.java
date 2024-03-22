package com.revolsys.jdbc.io;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;

import org.apache.commons.dbcp2.Jdbc41Bridge;
import org.apache.commons.dbcp2.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool2.PoolUtils;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectState;
import org.apache.commons.pool2.impl.DefaultPooledObject;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.value.ValueHolder;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.JdbcDataSource;
import com.revolsys.parallel.ReentrantLockEx;
import com.revolsys.parallel.ThreadUtil;
import com.revolsys.util.BaseCloseable;

public class JdbcDataSourceImpl extends JdbcDataSource {

  private static final Log log = LogFactory.getLog(JdbcDataSourceImpl.class);

  private static final Duration MAX_DURATION = Duration.ofMillis(Long.MAX_VALUE);

  private boolean accessToUnderlyingConnectionAllowed;
  private boolean cacheState = true;

  private boolean closed;

  private volatile ListEx<Consumer<Connection>> connectionInitCallbacks = ListEx.empty();

  private Properties connectionProperties = new Properties();

  private String url;

  private volatile Boolean defaultAutoCommit;

  private volatile String defaultCatalog;

  private Duration defaultQueryTimeoutDuration;

  private transient Boolean defaultReadOnly;
  private volatile String defaultSchema;

  private volatile int defaultTransactionIsolation = -1;

  private final ValueHolder<Driver> driverValue = ValueHolder.lazy(this::createDriver);

  private Driver driver;

  private ClassLoader driverClassLoader;

  private String driverClassName;

  private final ReentrantLockEx lock = new ReentrantLockEx();

  private Duration maxConnDuration = Duration.ofMillis(-1);

  private boolean rollbackOnReturn = true;

  private volatile String validationQuery;

  private volatile Duration validationQueryTimeoutDuration = Duration.ofSeconds(-1);

  private int minPoolSize = 0;

  private int maxPoolSize = 8;

  private Duration maxWait;

  private final LinkedList<PooledObject<PoolableConnection>> idleObjects = new LinkedList<>();
  private final Condition notEmpty = this.lock.newCondition();
  private final ReentrantLockEx makeObjectLock = new ReentrantLockEx();

  private final Condition makeObjectCondition = this.lock.newCondition();

  private final ConcurrentHashMap<PoolableConnection, PooledObject<PoolableConnection>> allObjects = new ConcurrentHashMap<>();

  private final AtomicLong createCount = new AtomicLong();

  private final ReentrantLockEx evictionLock = new ReentrantLockEx();

  private final int numTestsPerEvictionRun = 10;

  private PrintWriter logWriter;

  private final Duration idleEvictDuration = MAX_DURATION;

  private final Duration idleSoftEvictDuration = MAX_DURATION;

  private int minIdle = 0;

  private int maxIdle = -1;
  private boolean testWhileIdle;
  private long makeObjectCount = 0;

  private final Condition getConnectionCondition = this.lock.newCondition();

  private Duration minEvictableIdle;

  private Duration durationBetweenEvictionRuns;

  private Future<Void> evictor;

  public JdbcDataSourceImpl() {
    this.exceptionTranslator.setDataSource(this);
  }

  public void activateObject(final PooledObject<PoolableConnection> p) throws SQLException {

    validateLifetime(p);

    final PoolableConnection pConnection = p.getObject();
    pConnection.activate();

    if (JdbcDataSourceImpl.this.defaultAutoCommit != null
        && pConnection.getAutoCommit() != this.defaultAutoCommit) {
      pConnection.setAutoCommit(this.defaultAutoCommit);
    }
    final var defaultTransactionIsolation = getDefaultTransactionIsolation();
    if (defaultTransactionIsolation != -1
        && pConnection.getTransactionIsolation() != defaultTransactionIsolation) {
      pConnection.setTransactionIsolation(defaultTransactionIsolation);
    }
    if (this.defaultReadOnly != null
        && pConnection.isReadOnly() != this.defaultReadOnly) {
      pConnection.setReadOnly(this.defaultReadOnly);
    }
    if (this.defaultCatalog != null
        && !this.defaultCatalog.equals(pConnection.getCatalog())) {
      pConnection.setCatalog(this.defaultCatalog);
    }
    if (this.defaultSchema != null
        && !this.defaultSchema.equals(Jdbc41Bridge.getSchema(pConnection))) {
      Jdbc41Bridge.setSchema(pConnection, this.defaultSchema);
    }
    pConnection.setDefaultQueryTimeout(getDefaultQueryTimeoutDuration());
  }

  public void addConnectionProperty(final String name, final String value) {
    this.connectionProperties.put(name, value);
  }

  final void assertOpen() throws IllegalStateException {
    if (isClosed()) {
      throw new IllegalStateException("Pool not open");
    }
  }

  private Connection borrowObject(final Duration borrowMaxWaitDuration) {
    try (var l = this.lock.lockX()) {
      assertOpen();

      PooledObject<PoolableConnection> p = null;

      boolean create;
      final Instant waitTime = Instant.now();

      while (p == null) {
        create = false;
        p = this.idleObjects.pollFirst();
        if (p == null) {
          p = create();
          if (!PooledObject.isNull(p)) {
            create = true;
          }
        }
        if (PooledObject.isNull(p)) {
          if (borrowMaxWaitDuration.isNegative()) {
            p = this.idleObjects.removeFirst();
          } else {
            p = this.idleObjects.pollFirst(borrowMaxWaitDuration);
          }
        }
        if (PooledObject.isNull(p)) {
          throw new NoSuchElementException(
              "Timeout waiting for idle object, borrowMaxWaitDuration=" + borrowMaxWaitDuration);
        }
        if (!p.allocate()) {
          p = null;
        }

        if (!PooledObject.isNull(p)) {
          try {
            activateObject(p);
          } catch (final Exception e) {
            try {
              destroy(p);
            } catch (final Exception ignored) {
              // ignored - activation failure is more important
            }
            p = null;
            if (create) {
              final NoSuchElementException nsee = new NoSuchElementException(
                  "Unable to activate object");
              nsee.initCause(e);
              throw nsee;
            }
          }
        }
        return p.getObject();
      }
    }
  }

  public void clear() {
    while (true) {
      PooledObject<PoolableConnection> pooledObject;
      try (var l = this.lock.lockX()) {
        if (this.idleObjects.isEmpty()) {
          break;
        } else {
          pooledObject = this.idleObjects.removeFirst();
        }
      }
      try {
        destroy(pooledObject);
      } catch (final Exception e) {
        swallowException(e);
      }
    }
  }

  public void close() throws SQLException {
    try (var l = this.lock.lockX()) {
      this.closed = true;
      if (isClosed() || isClosed()) {
        return;
      }

      // Stop the evictor before the pool is closed since evict() calls
      // assertOpen()
      stopEvictor();

      this.closed = true;
      clear();

      this.getConnectionCondition.signalAll();
    }
  }

  private PooledObject<PoolableConnection> create() throws Exception {
    int maxPoolSize = getMaxPoolSize();
    if (maxPoolSize < 0) {
      maxPoolSize = Integer.MAX_VALUE;
    }

    final Instant localStartInstant = Instant.now();
    final Duration maxWait = getMaxWait();
    final Duration localMaxWaitDuration = maxWait.isNegative() ? Duration.ZERO : maxWait;

    // Flag that indicates if create should:
    // - TRUE: call the factory to create an object
    // - FALSE: return null
    // - null: loop and re-test the condition that determines whether to
    // call the factory
    Boolean create = null;
    while (create == null) {
      try (var l = this.makeObjectLock.lockX()) {
        final long newCreateCount = this.createCount.incrementAndGet();
        if (newCreateCount > maxPoolSize) {
          // The pool is currently at capacity or in the process of
          // making enough new objects to take it to capacity.
          this.createCount.decrementAndGet();
          if (this.makeObjectCount == 0) {
            // There are no makeObject() calls in progress so the
            // pool is at capacity. Do not attempt to create a new
            // object. Return and wait for an object to be returned
            create = Boolean.FALSE;
          } else {
            ThreadUtil.awaitDuration(this.makeObjectCondition, localMaxWaitDuration);
            this.makeObjectCondition.awaitNanos(localMaxWaitDuration.toNanos());
          }
        } else {
          // The pool is not at capacity. Create a new object.
          this.makeObjectCount++;
          create = Boolean.TRUE;
        }
      }

      // Do not block more if maxWaitTimeMillis is set.
      if (create == null && localMaxWaitDuration.compareTo(Duration.ZERO) > 0 &&
          Duration.between(localStartInstant, Instant.now()).compareTo(localMaxWaitDuration) >= 0) {
        create = Boolean.FALSE;
      }
    }

    if (!create.booleanValue()) {
      return null;
    }

    final PooledObject<PoolableConnection> p;
    try {
      p = makeObject();
      if (PooledObject.isNull(p)) {
        this.createCount.decrementAndGet();
        throw new NullPointerException("No connection created");
      }
    } catch (final Throwable e) {
      this.createCount.decrementAndGet();
      throw e;
    } finally {
      try (var l = this.makeObjectLock.lockX()) {
        this.makeObjectCount--;
        this.makeObjectCondition.signalAll();
      }
    }

    this.allObjects.put(p.getObject(), p);
    return p;
  }

  private Driver createDriver() {
    Driver driver;
    try {
      driver = getDriver();
      final String driverClassName = getDriverClassName();
      final ClassLoader driverClassLoader = getDriverClassLoader();
      final String url = getUrl();

      if (driver == null) {
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
              driverFromCCL = Thread.currentThread().getContextClassLoader().loadClass(driverClassName);
            }
          } catch (final Exception t) {
            final String message = "Cannot load JDBC driver class '" + driverClassName + "'";
            throw new SQLException(message, t);
          }
        }

        try {
          if (driverFromCCL == null) {
            driver = DriverManager.getDriver(url);
          } else {
            // Usage of DriverManager is not possible, as it does not
            // respect the ContextClassLoader
            // N.B. This cast may cause ClassCastException which is
            // handled below
            driver = (Driver) driverFromCCL.getConstructor().newInstance();
            if (!driver.acceptsURL(url)) {
              throw new SQLException("No suitable driver", "08001");
            }
          }
        } catch (final Exception t) {
          final String message = "Cannot create JDBC driver of class '"
              + (driverClassName != null ? driverClassName : "") + "' for connect URL '" + url + "'";
          throw new SQLException(message, t);
        }
      }
      validateConnectionFactory();
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
    return driver;
  }

  private void destroy(final PooledObject<PoolableConnection> toDestroy) {
    toDestroy.invalidate();
    this.idleObjects.remove(toDestroy);
    this.allObjects.remove(toDestroy.getObject());
    toDestroy.getObject().reallyClose();
  }

  private void ensureIdle(final int idleCount, final boolean always) throws Exception {
    if (idleCount < 1 || isClosed() || !always && !this.idleObjects.hasTakeWaiters()) {
      return;
    }

    while (this.idleObjects.size() < idleCount) {
      final PooledObject<PoolableConnection> p = create();
      if (PooledObject.isNull(p)) {
        // Can't create objects, no reason to think another call to
        // create will work. Give up.
        break;
      }
      this.idleObjects.addLast(p);
    }
    if (isClosed()) {
      clear();
    }
  }

  /**
   * Manually evicts idle connections
   *
   * @throws Exception when there is a problem evicting idle objects.
   */
  public void evict() throws Exception {
    assertOpen();

    if (!this.idleObjects.isEmpty()) {

      try (var l = this.lock.lockX()) {

        final boolean testWhileIdle = isTestWhileIdle();

        final var evictionIterator = this.idleObjects.iterator();

        for (int i = 0; i < this.numTestsPerEvictionRun && evictionIterator.hasNext(); i++) {
          final var underTest = evictionIterator.next();

          if (!underTest.startEvictionTest()) {
            // Object was borrowed in another thread
            // Don't count this as an eviction test so reduce i;
            i--;
            continue;
          }

          final int idleCount = this.idleObjects.size();
          final boolean evict = getIdleSoftEvictDuration().compareTo(underTest.getIdleDuration()) < 0 &&
              getMinIdle() < idleCount ||
              getIdleEvictDuration().compareTo(underTest.getIdleDuration()) < 0;

          if (evict) {
            destroy(underTest);
          } else {
            if (testWhileIdle) {
              boolean active = false;
              try {
                activateObject(underTest);
                active = true;
              } catch (final Exception e) {
                destroy(underTest);
              }
              if (active) {
                boolean validate = false;
                Throwable validationThrowable = null;
                try {
                  validate = validateObject(underTest);
                } catch (final Throwable t) {
                  PoolUtils.checkRethrow(t);
                  validationThrowable = t;
                }
                if (!validate) {
                  destroy(underTest);
                  if (validationThrowable != null) {
                    if (validationThrowable instanceof RuntimeException) {
                      throw (RuntimeException) validationThrowable;
                    }
                    throw (Error) validationThrowable;
                  }
                } else {
                  try {
                    passivateObject(underTest);
                  } catch (final Exception e) {
                    destroy(underTest);
                  }
                }
              }
            }
            underTest.endEvictionTest(this.idleObjects);
          }
        }
      }
    }
  }

  /**
   * Gets the state caching flag.
   *
   * @return the state caching flag
   */

  public boolean getCacheState() {
    return this.cacheState;
  }

  @Override
  public Connection getConnection(final String user, final String pass) {
    throw new UnsupportedOperationException("getConnection(user,password) is not supported");
  }

  public ListEx<Consumer<Connection>> getConnectionCallbacks() {
    return this.connectionInitCallbacks;
  }

  @Override
  protected Connection getConnectionInternal() throws SQLException {
    for (int i = 0; i < 2; i++) {
      try (var l = this.lock.lockX()) {
        try {
          return borrowObject(this.maxWait);
        } catch (final NoSuchElementException e) {
          if (i == 0
              && "Timeout waiting for idle object".equals(e.getMessage())) {
            // Retry once on timeout
          } else {
            throw e;
          }
        }
      }
    }
    return null;
  }

  Properties getConnectionProperties() {
    return this.connectionProperties;
  }

  public Boolean getDefaultAutoCommit() {
    return this.defaultAutoCommit;
  }

  public String getDefaultCatalog() {
    return this.defaultCatalog;
  }

  /**
   * Gets the default query timeout that will be used for
   * {@link java.sql.Statement Statement}s created from this
   * connection. {@code null} means that the driver default will be used.
   *
   * @return The default query timeout Duration.
   * @since 2.10.0
   */
  public Duration getDefaultQueryTimeoutDuration() {
    return this.defaultQueryTimeoutDuration;
  }

  /**
   * Gets the default readOnly property.
   *
   * @return true if connections are readOnly by default
   */

  public Boolean getDefaultReadOnly() {
    return this.defaultReadOnly;
  }

  /**
   * Gets the default schema.
   *
   * @return the default schema.
   * @since 2.5.0
   */

  public String getDefaultSchema() {
    return this.defaultSchema;
  }

  /**
   * Gets the default transaction isolation state of returned connections.
   *
   * @return the default value for transaction isolation state
   * @see Connection#getTransactionIsolation
   */

  public int getDefaultTransactionIsolation() {
    return this.defaultTransactionIsolation;
  }

  /**
   * Gets the JDBC Driver that has been configured for use by this pool.
   * <p>
   * Note: This getter only returns the last value set by a call to
   * {@link #setDriver(Driver)}. It does not return any
   * driver instance that may have been created from the value set via
   * {@link #setDriverClassName(String)}.
   * </p>
   *
   * @return the JDBC Driver that has been configured for use by this pool
   */
  public Driver getDriver() {
    return this.driver;
  }

  /**
   * Gets the class loader specified for loading the JDBC driver. Returns
   * {@code null} if no class loader has
   * been explicitly specified.
   * <p>
   * Note: This getter only returns the last value set by a call to
   * {@link #setDriverClassLoader(ClassLoader)}. It
   * does not return the class loader of any driver that may have been set via
   * {@link #setDriver(Driver)}.
   * </p>
   *
   * @return The class loader specified for loading the JDBC driver.
   */
  public ClassLoader getDriverClassLoader() {
    return this.driverClassLoader;
  }

  /**
   * Gets the JDBC driver class name.
   * <p>
   * Note: This getter only returns the last value set by a call to
   * {@link #setDriverClassName(String)}. It does not
   * return the class name of any driver that may have been set via
   * {@link #setDriver(Driver)}.
   * </p>
   *
   * @return the JDBC driver class name
   */

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

  /**
   * Gets the maximum permitted duration of a connection. A value of zero or less
   * indicates an
   * infinite lifetime.
   *
   * @return the maximum permitted duration of a connection.
   * @since 2.10.0
   */
  public Duration getMaxConnDuration() {
    return this.maxConnDuration;
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

  PooledObject<PoolableConnection> getPooledObject(final PoolableConnection obj) {
    return this.allObjects.get(obj);
  }

  public String getUrl() {
    return this.url;
  }

  /**
   * Gets the validation query used to validate connections before returning them.
   *
   * @return the SQL validation query
   * @see #setValidationQuery(String)
   */

  public String getValidationQuery() {
    return this.validationQuery;
  }

  /**
   * Gets the validation query timeout.
   *
   * @return the timeout in seconds before connection validation queries fail.
   */
  public Duration getValidationQueryTimeoutDuration() {
    return this.validationQueryTimeoutDuration;
  }

  /**
   * Manually invalidates a connection, effectively requesting the pool to try to
   * close it, remove it from the pool
   * and reclaim pool capacity.
   *
   * @param connection The Connection to invalidate.
   *
   * @throws IllegalStateException if invalidating the connection failed.
   * @since 2.1
   */
  @SuppressWarnings("resource")
  public void invalidateConnection(final Connection connection) throws IllegalStateException {
    if (connection == null) {
      return;
    }

    final PoolableConnection poolableConnection;
    try {
      poolableConnection = connection.unwrap(PoolableConnection.class);
      if (poolableConnection == null) {
        throw new IllegalStateException(
            "Cannot invalidate connection: Connection is not a poolable connection.");
      }
    } catch (final SQLException e) {
      throw new IllegalStateException("Cannot invalidate connection: Unwrapping poolable connection failed.", e);
    }

    try {
      invalidateObject(poolableConnection);
    } catch (final Exception e) {
      throw new IllegalStateException("Invalidating connection threw unexpected exception", e);
    }
  }

  public void invalidateObject(final PoolableConnection obj) throws Exception {
    final var p = getPooledObject(obj);
    if (p == null) {
      throw new IllegalStateException("Invalidated object not currently part of this pool");
    }
    synchronized (p) {
      if (p.getState() != PooledObjectState.INVALID) {
        destroy(p);
      }
    }
    ensureIdle(1, false);
  }

  public boolean isAccessToUnderlyingConnectionAllowed() {
    return this.accessToUnderlyingConnectionAllowed;
  }

  public boolean isClosed() {
    return this.closed;
  }

  private boolean isEmpty(final String value) {
    return value == null || value.trim().isEmpty();
  }

  public boolean isRollbackOnReturn() {
    return this.rollbackOnReturn;
  }

  public boolean isTestWhileIdle() {
    return this.testWhileIdle;
  }

  @Override
  public boolean isWrapperFor(final Class<?> iface) throws SQLException {
    return iface != null && iface.isInstance(this);
  }

  public PooledObject<PoolableConnection> makeObject() throws SQLException {
    final var url = getUrl();
    final var connectionProperties = getConnectionProperties();
    final var connection = this.driverValue.getValue().connect(url, connectionProperties);
    if (connection == null) {
      throw new SQLException("Unable to create connection");
    }
    try {
      for (final Consumer<Connection> callback : JdbcDataSourceImpl.this.connectionInitCallbacks) {
        callback.accept(connection);
      }
    } catch (final RuntimeException | Error e) {
      BaseCloseable.closeSilent(connection);
      throw e;
    }

    final PoolableConnection pc = new PoolableConnection(connection, this);
    pc.setCacheState(getCacheState());

    return new DefaultPooledObject<>(pc);
  }

  protected void markReturningState(final PooledObject<PoolableConnection> pooledObject) {

    if (pooledObject.getState() != PooledObjectState.ALLOCATED) {
      throw new IllegalStateException("Object has already been returned to this pool or is invalid");
    }
    pooledObject.markReturning(); // Keep from being marked abandoned

  }

  public void passivateObject(final PooledObject<PoolableConnection> p) throws SQLException {

    validateLifetime(p);

    final PoolableConnection connection = p.getObject();
    Boolean connAutoCommit = null;
    if (isRollbackOnReturn()) {
      connAutoCommit = connection.getAutoCommit();
      if (!connAutoCommit && !connection.isReadOnly()) {
        connection.rollback();
      }
    }

    connection.clearWarnings();

    if (connAutoCommit == null) {
      connAutoCommit = connection.getAutoCommit();
    }
    if (!connAutoCommit) {
      connection.setAutoCommit(true);
    }

    connection.passivate();
  }

  public void removeConnectionProperty(final String name) {
    this.connectionProperties.remove(name);
  }

  public void returnObject(final PoolableConnection obj) {
    final PooledObject<PoolableConnection> p = getPooledObject(obj);

    if (p == null) {
      return; // Object was abandoned and removed
    }

    markReturningState(p);

    final Duration activeTime = p.getActiveDuration();

    try {
      passivateObject(p);
    } catch (final Exception e1) {
      swallowException(e1);
      try {
        destroy(p);
      } catch (final Exception e) {
        swallowException(e);
      }
      try {
        ensureIdle(1, false);
      } catch (final Exception e) {
        swallowException(e);
      }
      return;
    }

    if (!p.deallocate()) {
      throw new IllegalStateException(
          "Object has already been returned to this pool or is invalid");
    }

    final int maxIdleSave = getMaxIdle();
    if (isClosed() || maxIdleSave > -1 && maxIdleSave <= this.idleObjects.size()) {
      try {
        destroy(p);
      } catch (final Exception e) {
        swallowException(e);
      }
      try {
        ensureIdle(1, false);
      } catch (final Exception e) {
        swallowException(e);
      }
    } else {
      this.idleObjects.addLast(p);
      if (isClosed()) {
        clear();
      }
    }
  }

  public JdbcDataSourceImpl setAccessToUnderlyingConnectionAllowed(final boolean allow) {
    this.accessToUnderlyingConnectionAllowed = allow;
    return this;
  }

  public JdbcDataSourceImpl setCacheState(final boolean cacheState) {
    this.cacheState = cacheState;
    return this;
  }

  public JdbcDataSourceImpl setConnectionInitCallbacks(final ListEx<Consumer<Connection>> connectionInitCallbacks) {
    this.connectionInitCallbacks = connectionInitCallbacks;
    return this;
  }

  public void setConnectionProperties(final String connectionProperties) {
    Objects.requireNonNull(connectionProperties, "connectionProperties");
    final String[] entries = connectionProperties.split(";");
    final Properties properties = new Properties();
    Stream.of(entries).filter(e -> !e.isEmpty()).forEach(entry -> {
      final int index = entry.indexOf('=');
      if (index > 0) {
        final String name = entry.substring(0, index);
        final String value = entry.substring(index + 1);
        properties.setProperty(name, value);
      } else {
        // no value is empty string which is how
        // java.util.Properties works
        properties.setProperty(entry, "");
      }
    });
    this.connectionProperties = properties;
  }

  public JdbcDataSourceImpl setDefaultAutoCommit(final Boolean defaultAutoCommit) {
    this.defaultAutoCommit = defaultAutoCommit;
    return this;
  }

  public JdbcDataSourceImpl setDefaultCatalog(final String defaultCatalog) {
    this.defaultCatalog = isEmpty(defaultCatalog) ? null : defaultCatalog;
    return this;
  }

  public JdbcDataSourceImpl setDefaultQueryTimeout(final Duration defaultQueryTimeoutDuration) {
    this.defaultQueryTimeoutDuration = defaultQueryTimeoutDuration;
    return this;
  }

  public JdbcDataSourceImpl setDefaultReadOnly(final Boolean defaultReadOnly) {
    this.defaultReadOnly = defaultReadOnly;
    return this;
  }

  public JdbcDataSourceImpl setDefaultSchema(final String defaultSchema) {
    this.defaultSchema = isEmpty(defaultSchema) ? null : defaultSchema;
    return this;
  }

  public JdbcDataSourceImpl setDefaultTransactionIsolation(final int defaultTransactionIsolation) {
    this.defaultTransactionIsolation = defaultTransactionIsolation;
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
    this.driverClassName = isEmpty(driverClassName) ? null : driverClassName;
    return this;
  }

  public JdbcDataSourceImpl setDurationBetweenEvictionRuns(final Duration durationBetweenEvictionRuns) {
    this.durationBetweenEvictionRuns = durationBetweenEvictionRuns;
    return this;
  }

  /**
   * <strong>BasicDataSource does NOT support this method. </strong>
   *
   * <p>
   * Sets the login timeout (in seconds) for connecting to the database.
   * </p>
   * <p>
   * Calls {@link #createDataSource()}, so has the side effect of initializing the
   * connection pool.
   * </p>
   *
   * @param loginTimeout The new login timeout, or zero for no timeout
   * @throws UnsupportedOperationException If the DataSource implementation does
   *                                       not support the login timeout
   *                                       feature.
   * @throws SQLException                  if a database access error occurs
   */

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

  /**
   * Sets the maximum permitted lifetime of a connection. A value of zero or less
   * indicates an
   * infinite lifetime.
   * <p>
   * Note: this method currently has no effect once the pool has been initialized.
   * The pool is initialized the first
   * time one of the following methods is invoked:
   * <code>getConnection, setLogwriter,
   * setLoginTimeout, getLoginTimeout, getLogWriter.</code>
   * </p>
   *
   * @param maxConnDuration The maximum permitted lifetime of a connection.
   * @since 2.10.0
   */
  public void setMaxConn(final Duration maxConnDuration) {
    this.maxConnDuration = maxConnDuration;
  }

  public JdbcDataSourceImpl setMaxIdle(final int maxIdle) {
    this.maxIdle = maxIdle;
    return this;
  }

  public JdbcDataSourceImpl setMaxPoolSize(final int maxPoolSize) {
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

  public JdbcDataSourceImpl setRollbackOnReturn(final boolean rollbackOnReturn) {
    this.rollbackOnReturn = rollbackOnReturn;
    return this;
  }

  public JdbcDataSourceImpl setUrl(final String url) {
    this.url = url;
    return this;
  }

  public JdbcDataSourceImpl setValidationQuery(final String validationQuery) {
    this.validationQuery = isEmpty(validationQuery) ? null : validationQuery;
    return this;
  }

  public JdbcDataSourceImpl setValidationQueryTimeout(final Duration validationQueryTimeoutDuration) {
    this.validationQueryTimeoutDuration = validationQueryTimeoutDuration;
    return this;
  }

  final void startEvictor(final Duration delay) {
    synchronized (this.evictionLock) {
      final boolean isPositiverDelay = PoolImplUtils.isPositive(delay);
      if (this.evictor == null) { // Starting evictor for the first time or after a cancel
        if (isPositiverDelay) { // Starting new evictor
          this.evictor = new Evictor();
          EvictionTimer.schedule(this.evictor, delay, delay);
        }
      } else if (isPositiverDelay) { // Stop or restart of existing evictor: Restart
        synchronized (EvictionTimer.class) { // Ensure no cancel can happen between cancel / schedule calls
          EvictionTimer.cancel(this.evictor, evictorShutdownTimeoutDuration, true);
          this.evictor = null;
          evictionIterator = null;
          this.evictor = new Evictor();
          EvictionTimer.schedule(this.evictor, delay, delay);
        }
      } else { // Stopping evictor
        EvictionTimer.cancel(this.evictor, evictorShutdownTimeoutDuration, false);
      }
    }
  }

  private void swallowException(final Throwable e) {
  }

  @Override
  public <T> T unwrap(final Class<T> iface) throws SQLException {
    if (isWrapperFor(iface)) {
      return iface.cast(this);
    }
    throw new SQLException(this + " is not a wrapper for " + iface);
  }

  /**
   * Validates the given connection if it is open.
   *
   * @param conn the connection to validate.
   * @throws SQLException if the connection is closed or validate fails.
   */
  public void validateConnection(final PoolableConnection conn) throws SQLException {
    if (conn.isClosed()) {
      throw new SQLException("validateConnection: connection closed");
    }
    conn.validate(JdbcDataSourceImpl.this.validationQuery, JdbcDataSourceImpl.this.validationQueryTimeoutDuration);
  }

  @SuppressWarnings("resource")
  protected void validateConnectionFactory()
      throws SQLException {
    final var p = makeObject();
    try {
      final var connection = p.getObject();
      activateObject(p);
      validateConnection(connection);
      passivateObject(p);
    } finally {
      if (p != null) {
        destroy(p);
      }
    }
  }

  private void validateLifetime(final PooledObject<PoolableConnection> p) throws LifetimeExceededException {
    final var maxConnDuration = getMaxConnDuration();
    if (maxConnDuration.compareTo(Duration.ZERO) > 0) {
      final Duration lifetimeDuration = Duration.between(p.getCreateInstant(), Instant.now());
      if (lifetimeDuration.compareTo(maxConnDuration) > 0) {
        throw new LifetimeExceededException(
            Utils.getMessage("connectionFactory.lifetimeExceeded", lifetimeDuration, maxConnDuration));
      }
    }
  }

  public boolean validateObject(final PooledObject<PoolableConnection> p) {
    try {
      validateLifetime(p);
      validateConnection(p.getObject());
      return true;
    } catch (final Exception e) {
      if (log.isDebugEnabled()) {
        log.debug(Utils.getMessage("poolableConnectionFactory.validateObject.fail"), e);
      }
      return false;
    }
  }

}