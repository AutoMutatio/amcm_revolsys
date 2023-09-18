package com.revolsys.transaction;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import javax.sql.DataSource;

import org.jeometry.common.exception.Exceptions;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.jdbc.datasource.ConnectionProxy;
import org.springframework.jdbc.datasource.SmartDataSource;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;
import org.springframework.lang.Nullable;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidTimeoutException;
import org.springframework.transaction.ReactiveTransaction;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.AbstractReactiveTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronization;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import org.springframework.transaction.reactive.TransactionalOperator;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

public class DataSourceReactiveTransactionManager extends AbstractReactiveTransactionManager {

  private static class DataSourceTransactionState {

    private boolean newConnectionHolder;

    private boolean mustRestoreAutoCommit;

    @Nullable
    private ReactiveConnectionHolder connectionHolder;

    @Nullable
    private Integer previousIsolationLevel;

    private boolean readOnly = false;

    public DataSourceTransactionState(final ReactiveConnectionHolder connectionHolder) {
      this.connectionHolder = connectionHolder;
    }

    public void clearConnection() {
      this.connectionHolder = null;
    }

    public ReactiveConnectionHolder getConnectionHolder() {
      return this.connectionHolder;
    }

    public Integer getPreviousIsolationLevel() {
      return this.previousIsolationLevel;
    }

    public boolean isMustRestoreAutoCommit() {
      return this.mustRestoreAutoCommit;
    }

    public boolean isNewConnectionHolder() {
      return this.newConnectionHolder;
    }

    public boolean isReadOnly() {
      return this.readOnly;
    }

    public void setConnectionHolder(@Nullable final ReactiveConnectionHolder connectionHolder) {
      this.connectionHolder = connectionHolder;
      this.newConnectionHolder = true;
    }

    public void setMustRestoreAutoCommit(final boolean mustRestoreAutoCommit) {
      this.mustRestoreAutoCommit = mustRestoreAutoCommit;
    }

    public void setPreviousIsolationLevel(@Nullable final Integer previousIsolationLevel) {
      this.previousIsolationLevel = previousIsolationLevel;
    }

    public void setReadOnly(final boolean readOnly) {
      this.readOnly = readOnly;
    }

  }

  private class ReactiveConnectionHolder extends ConnectionHolder {

    private class ConnectionSynchronization implements TransactionSynchronization {

      private boolean holderActive = true;

      private final TransactionSynchronizationManager synchronizationManager;

      public ConnectionSynchronization(
        final TransactionSynchronizationManager synchronizationManager) {
        this.synchronizationManager = synchronizationManager;
      }

      @Override
      public Mono<Void> afterCompletion(final int status) {
        if (this.holderActive) {
          final DataSource dataSource = getDataSource();
          this.synchronizationManager.unbindResourceIfPossible(dataSource);
          this.holderActive = false;
          if (hasConnection()) {
            releaseConnection(this.synchronizationManager,
              ReactiveConnectionHolder.this.getConnection());
            setConnection(null);
          }
        }
        reset();
        return Mono.empty();
      }

      @Override
      public Mono<Void> beforeCompletion() {
        if (!isOpen()) {
          this.synchronizationManager.unbindResource(getDataSource());
          this.holderActive = false;
          if (hasConnection()) {
            releaseConnection(this.synchronizationManager,
              ReactiveConnectionHolder.this.getConnection());
          }
        }
        return Mono.empty();
      }

      @Override
      public Mono<Void> resume() {
        if (this.holderActive) {
          this.synchronizationManager.bindResource(getDataSource(), ReactiveConnectionHolder.this);
        }
        return Mono.empty();
      }

      @Override
      public Mono<Void> suspend() {
        if (this.holderActive) {
          this.synchronizationManager.unbindResource(getDataSource());
          if (hasConnection() && !isOpen()) {
            // Release Connection on suspend if the application doesn't keep
            // a handle to it anymore. We will fetch a fresh Connection if the
            // application accesses the ConnectionHolder again after resume,
            // assuming that it will participate in the same transaction.
            releaseConnection(this.synchronizationManager,
              ReactiveConnectionHolder.this.getConnection());
            setConnection(null);
          }
        }
        return Mono.empty();
      }
    }

    public ReactiveConnectionHolder(final Connection connection) throws SQLException {
      super(connection);
    }

    @SuppressWarnings("resource")
    public boolean connectionEquals(final Connection connection) {
      if (hasConnection()) {
        final Connection heldCon = getConnection();
        if (heldCon == connection || heldCon.equals(connection)) {
          return true;
        } else {
          Connection conToUse = heldCon;
          while (conToUse instanceof final ConnectionProxy connectionProxy) {
            conToUse = connectionProxy.getTargetConnection();
          }
          return conToUse.equals(connection);
        }
      }
      return false;
    }

    @Override
    public boolean hasConnection() {
      return super.hasConnection();
    }

    @Override
    public boolean isTransactionActive() {
      return super.isTransactionActive();
    }

    public ConnectionSynchronization newSynchronization(
      final TransactionSynchronizationManager synchronizationManager) {
      requested();
      final ConnectionSynchronization synchronization = new ConnectionSynchronization(
        synchronizationManager);
      synchronizationManager.registerSynchronization(synchronization);
      setSynchronizedWithTransaction(true);
      return synchronization;
    }

    @Override
    public void setConnection(final Connection connection) {
      super.setConnection(connection);
    }

    @Override
    public void setTransactionActive(final boolean transactionActive) {
      super.setTransactionActive(transactionActive);
    }
  }

  private final TransactionalOperator defaultOperator = TransactionalOperator.create(this,
    com.revolsys.transaction.TransactionDefinition.DEFAULT);

  private final TransactionalOperator requiredOperator = TransactionalOperator.create(this,
    com.revolsys.transaction.TransactionDefinition.REQUIRED);

  private final DataSource dataSource;

  private boolean enforceReadOnly = false;

  private int defaultTimeout = TransactionDefinition.TIMEOUT_DEFAULT;

  private final Consumer<Connection> connectionInitializer;

  public DataSourceReactiveTransactionManager(final DataSource dataSource,
    final Consumer<Connection> connectionInitializer) {
    this.dataSource = dataSource;
    this.connectionInitializer = connectionInitializer;
  }

  protected void closeConnection(final DataSource dataSource, final Connection connection) {
    if (!(dataSource instanceof final SmartDataSource smartDataSource)
      || smartDataSource.shouldClose(connection)) {
      try {
        connection.close();
      } catch (final SQLException e) {
        throw Exceptions.wrap(e);
      }
    }
  }

  protected void connectionTransactionInit(final ContextView context, final Connection connection) {
    if (context.hasKey("jdbcConnectionInitializer")) {
      final Consumer<Connection> connectionInitializer = context.get("jdbcConnectionInitializer");
      connectionInitializer.accept(connection);
    }
  }

  protected int determineTimeout(final TransactionDefinition definition) {
    if (definition.getTimeout() != TransactionDefinition.TIMEOUT_DEFAULT) {
      return definition.getTimeout();
    }
    return getDefaultTimeout();
  }

  @Override
  protected Mono<Void> doBegin(final TransactionSynchronizationManager synchronizationManager,
    final Object transaction, final TransactionDefinition definition) {
    return Mono.deferContextual(
      context -> doBegin(synchronizationManager, transaction, definition, context));
  }

  private Mono<? extends Void> doBegin(
    final TransactionSynchronizationManager synchronizationManager, final Object transaction,
    final TransactionDefinition definition, final ContextView context) {
    final DataSourceTransactionState tx = getTransactionState(transaction);
    Connection connection = null;

    try {
      ReactiveConnectionHolder connectionHolder = tx.getConnectionHolder();
      if (connectionHolder == null || connectionHolder.isSynchronizedWithTransaction()) {
        connection = fetchConnection(this.dataSource);
        connectionHolder = new ReactiveConnectionHolder(connection);
        tx.setConnectionHolder(connectionHolder);
      }

      connectionHolder.setSynchronizedWithTransaction(true);
      connection = tx.getConnectionHolder().getConnection();

      if (definition != null && definition.isReadOnly()) {
        try {
          connection.setReadOnly(true);
        } catch (SQLException | RuntimeException ex) {
          Throwable exToCheck = ex;
          while (exToCheck != null) {
            if (exToCheck.getClass().getSimpleName().contains("Timeout")) {
              // Assume it's a connection timeout that would otherwise get
              // lost: e.g. from JDBC 4.0
              return Mono.error(ex);
            }
            exToCheck = exToCheck.getCause();
          }
        }
      }

      // Apply specific isolation level, if any.
      Integer previousIsolationLevel = null;
      if (definition != null
        && definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT) {
        final int currentIsolation = connection.getTransactionIsolation();
        if (currentIsolation != definition.getIsolationLevel()) {
          previousIsolationLevel = currentIsolation;
          connection.setTransactionIsolation(definition.getIsolationLevel());
        }
      }

      tx.setPreviousIsolationLevel(previousIsolationLevel);
      tx.setReadOnly(definition.isReadOnly());

      if (connection.getAutoCommit()) {
        tx.setMustRestoreAutoCommit(true);
        connection.setAutoCommit(false);
      }
      connectionTransactionInit(context, connection);
      prepareTransactionalConnection(connection, definition);
      connectionHolder.setTransactionActive(true);

      final int timeout = determineTimeout(definition);
      if (timeout != TransactionDefinition.TIMEOUT_DEFAULT) {
        connectionHolder.setTimeoutInSeconds(timeout);
      }
      if (tx.isNewConnectionHolder()) {
        synchronizationManager.bindResource(this.dataSource, connectionHolder);
      }
      return Mono.empty();
    } catch (final Throwable ex) {
      if (tx.isNewConnectionHolder()) {
        releaseConnection(synchronizationManager, connection);
        tx.clearConnection();
      }
      return Mono.error(
        new CannotCreateTransactionException("Could not open JDBC Connection for transaction", ex));
    }
  }

  @Override
  protected Mono<Void> doCleanupAfterCompletion(
    final TransactionSynchronizationManager synchronizationManager, final Object transaction) {
    final DataSourceTransactionState tx = getTransactionState(transaction);

    if (tx.isNewConnectionHolder()) {
      synchronizationManager.unbindResource(this.dataSource);
    }

    final ReactiveConnectionHolder connectionHolder = tx.getConnectionHolder();
    final Connection connection = connectionHolder.getConnection();
    try {
      if (tx.isMustRestoreAutoCommit()) {
        connection.setAutoCommit(true);
      }
      final Integer previousIsolationLevel = tx.getPreviousIsolationLevel();
      if (previousIsolationLevel != null) {
        connection.setTransactionIsolation(previousIsolationLevel);
      }

      if (tx.isReadOnly()) {
        connection.setReadOnly(false);
      }
    } catch (final Throwable ex) {
      this.logger.debug("Could not reset JDBC Connection after transaction", ex);
    }

    if (tx.isNewConnectionHolder()) {
      releaseConnection(synchronizationManager, connection);
    }
    connectionHolder.clear();
    return Mono.empty();
  }

  @Override
  protected Mono<Void> doCommit(final TransactionSynchronizationManager synchronizationManager,
    final GenericReactiveTransaction status) {
    return Mono.defer(() -> {
      try {
        getConnection(status).commit();
        return Mono.empty();
      } catch (final SQLException ex) {
        return translateException("JDBC commit", ex);
      }
    });
  }

  private Connection doGetConnection(final ContextView context,
    final TransactionSynchronizationManager synchronizationManager) throws SQLException {
    final DataSource dataSource = getDataSource();
    final ReactiveConnectionHolder holder = (ReactiveConnectionHolder)synchronizationManager
      .getResource(dataSource);
    if (holder != null && (holder.hasConnection() || holder.isSynchronizedWithTransaction())) {
      holder.requested();
      if (!holder.hasConnection()) {
        holder.setConnection(fetchConnection(dataSource));
      }
      return holder.getConnection();
    }
    final Connection connection = fetchConnection(dataSource);

    if (synchronizationManager.isSynchronizationActive()) {
      try {
        ReactiveConnectionHolder holderToSynchronize = holder;
        if (holderToSynchronize == null) {
          holderToSynchronize = new ReactiveConnectionHolder(connection);
        } else {
          holderToSynchronize.setConnection(connection);
        }
        holderToSynchronize.newSynchronization(synchronizationManager);
        if (holderToSynchronize != holder) {
          synchronizationManager.bindResource(dataSource, holderToSynchronize);
        }
      } catch (final RuntimeException ex) {
        releaseConnection(synchronizationManager, connection);
        throw ex;
      }
    }

    return connection;
  }

  @Override
  protected Object doGetTransaction(
    final TransactionSynchronizationManager synchronizationManager) {
    final ReactiveConnectionHolder conHolder = (ReactiveConnectionHolder)synchronizationManager
      .getResource(this.dataSource);
    return new DataSourceTransactionState(conHolder);
  }

  @Override
  protected Mono<Void> doResume(final TransactionSynchronizationManager synchronizationManager,
    final Object transaction, final Object suspendedResources) {
    synchronizationManager.bindResource(this.dataSource, suspendedResources);
    return Mono.empty();
  }

  @Override
  protected Mono<Void> doRollback(final TransactionSynchronizationManager synchronizationManager,
    final GenericReactiveTransaction status) {
    return Mono.defer(() -> {
      try {
        getConnection(status)//
          .rollback();
        return Mono.empty();
      } catch (final SQLException ex) {
        return translateException("JDBC rollback", ex);
      }
    });
  }

  @Override
  protected Mono<Void> doSetRollbackOnly(
    final TransactionSynchronizationManager synchronizationManager,
    final GenericReactiveTransaction status) {
    getConnectionHolder(status).setRollbackOnly();
    return Mono.empty();
  }

  @Override
  protected Mono<Object> doSuspend(final TransactionSynchronizationManager synchronizationManager,
    final Object transaction) {
    getTransactionState(transaction) //
      .clearConnection();
    return Mono.just(synchronizationManager.unbindResource(this.dataSource));
  }

  private Connection fetchConnection(final DataSource dataSource) throws SQLException {
    final Connection connection = dataSource.getConnection();
    if (connection == null) {
      throw new IllegalStateException(
        "DataSource returned null from getConnection(): " + dataSource);
    }
    this.connectionInitializer.accept(connection);
    return connection;
  }

  private Connection getConnection(final GenericReactiveTransaction status) {
    return getConnectionHolder(status).getConnection();
  }

  private ReactiveConnectionHolder getConnectionHolder(final GenericReactiveTransaction status) {
    return getTransactionState(status).getConnectionHolder();
  }

  DataSource getDataSource() {
    return this.dataSource;
  }

  public final int getDefaultTimeout() {
    return this.defaultTimeout;
  }

  private DataSourceTransactionState getTransactionState(final GenericReactiveTransaction status) {
    return (DataSourceTransactionState)status.getTransaction();
  }

  private DataSourceTransactionState getTransactionState(final Object transaction) {
    return (DataSourceTransactionState)transaction;
  }

  public boolean isEnforceReadOnly() {
    return this.enforceReadOnly;
  }

  public TransactionalOperator operator() {
    return this.defaultOperator;
  }

  public TransactionalOperator operator(final TransactionDefinition definition) {
    return TransactionalOperator.create(this, definition);
  }

  public TransactionalOperator operatorRequired() {
    return this.requiredOperator;
  }

  protected void prepareTransactionalConnection(final Connection con,
    final TransactionDefinition definition) throws SQLException {
    if (isEnforceReadOnly() && definition.isReadOnly()) {
      try (
        Statement stmt = con.createStatement()) {
        stmt.executeUpdate("SET TRANSACTION READ ONLY");
      }
    }
  }

  public void releaseConnection(final TransactionSynchronizationManager synchronizationManager,
    final Connection connection) {
    final DataSource dataSource = getDataSource();
    if (connection != null) {
      final ReactiveConnectionHolder connectionHolder = (ReactiveConnectionHolder)synchronizationManager
        .getResource(dataSource);
      if (connectionHolder != null && connectionHolder.connectionEquals(connection)) {
        connectionHolder.released();
        return;
      }
      closeConnection(dataSource, connection);
    }
  }

  public final void setDefaultTimeout(final int defaultTimeout) {
    if (defaultTimeout < TransactionDefinition.TIMEOUT_DEFAULT) {
      throw new InvalidTimeoutException("Invalid default timeout", defaultTimeout);
    }
    this.defaultTimeout = defaultTimeout;
  }

  public void setEnforceReadOnly(final boolean enforceReadOnly) {
    this.enforceReadOnly = enforceReadOnly;
  }

  private <V> Mono<V> translateException(final String task, final SQLException ex) {
    return Mono
      .error(new SQLErrorCodeSQLExceptionTranslator(this.dataSource).translate(task, null, ex));
  }

  public <V> Flux<V> withConnection(
    final BiFunction<ReactiveTransaction, Connection, Mono<V>> mapper) {
    return operatorRequired()
      .execute(transaction -> TransactionSynchronizationManager.forCurrentTransaction()
        .flatMap(synchronizationManager -> Mono.deferContextual(
          context -> Mono.using(() -> doGetConnection(context, synchronizationManager),
            connection -> mapper.apply(transaction, connection),
            connection -> releaseConnection(synchronizationManager, connection)))));

  }

}
