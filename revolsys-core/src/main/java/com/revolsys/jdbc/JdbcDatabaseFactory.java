package com.revolsys.jdbc;

import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.io.IoFactory;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.io.RecordStoreFactory;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;
import com.revolsys.util.PasswordUtil;
import com.revolsys.util.Property;

public interface JdbcDatabaseFactory extends RecordStoreFactory {
  String URL_FIELD = "urlField";

  static DataSource closeDataSource(final DataSource dataSource) {
    if (dataSource instanceof final JdbcDataSourceImpl ds) {
      ds.close();
    }
    return null;
  }

  static List<JdbcDatabaseFactory> databaseFactories() {
    return IoFactory.factories(JdbcDatabaseFactory.class);
  }

  static JdbcDatabaseFactory databaseFactory(final Map<String, ? extends Object> config) {
    final String url = (String)config.get("url");
    if (url == null) {
      throw new IllegalArgumentException("The url parameter must be specified");
    } else {
      for (final JdbcDatabaseFactory databaseFactory : databaseFactories()) {
        if (databaseFactory.canOpenUrl(url)) {
          return databaseFactory;
        }
      }
      throw new IllegalArgumentException("Database factory not found for " + url);
    }
  }

  static JdbcDatabaseFactory databaseFactory(final String productName) {
    for (final JdbcDatabaseFactory databaseFactory : databaseFactories()) {
      if (databaseFactory.getProductName()
        .equals(productName)) {
        return databaseFactory;
      }
    }
    return null;
  }

  static JdbcDataSource dataSource(final Map<String, Object> config) {
    final JdbcDatabaseFactory databaseFactory = JdbcDatabaseFactory.databaseFactory(config);
    return databaseFactory.newDataSource(config);
  }

  static DataSource dataSource(final String url, final String username, final String password) {
    final Map<String, Object> config = new HashMap<>();
    config.put("url", url);
    config.put("user", username);
    config.put("password", password);
    return dataSource(config);
  }

  @Override
  default boolean canOpenPath(final Path path) {
    return false;
  }

  @Override
  default boolean canOpenUrl(final String url) {
    if (url.startsWith("jdbc:" + getVendorName() + ":")) {
      return true;
    } else {
      return false;
    }
  }

  List<FieldDefinition> getConnectionFieldDefinitions();

  /**
   * Get the map from connection name to JDBC URL for the database driver. For
   * example in Oracle this will be connections loaded from the TNSNAMES.ora file.
   *
   * @return
   */
  default Map<String, String> getConnectionUrlMap() {
    return Collections.emptyMap();
  }

  default String getConnectionValidationQuery() {
    return "SELECT 1";
  }

  String getDriverClassName();

  String getProductName();

  @Override
  Class<? extends RecordStore> getRecordStoreInterfaceClass(
    Map<String, ? extends Object> connectionProperties);

  @Override
  default List<Pattern> getUrlPatterns() {
    return Collections.singletonList(Pattern.compile("jdbc:" + getVendorName() + ":.+"));
  }

  String getVendorName();

  @Override
  default boolean isAvailable() {
    return true;
  }

  @SuppressWarnings({
    "unchecked"
  })
  default JdbcDataSource newDataSource(final Map<String, ? extends Object> config) {
    try {
      final MapEx newConfig = JsonObject.hash(config);
      final String url = (String)newConfig.remove("url");
      final Duration maxAge = removeDuration(newConfig, "maxAge", Duration.ofMinutes(60));
      final int maxPoolSize = removeInteger(newConfig, "maxPoolSize", 10);
      final int maxIdle = removeInteger(newConfig, "maxIdle", maxPoolSize);
      final Duration maxWait = removeDuration(newConfig, "maxWait", Duration.ofSeconds(1));
      Supplier<String> userSupplier;
      final var user = newConfig.remove("user");
      {
        if (user instanceof final Supplier supplier) {
          userSupplier = supplier;
        } else if (user != null) {
          final var s = user.toString();
          userSupplier = () -> s;
        } else {
          userSupplier = null;
        }
      }
      Supplier<String> passwordSupplier;
      {
        final var password = newConfig.remove("password");
        if (password instanceof final Supplier supplier) {
          passwordSupplier = supplier;
        } else if (Property.hasValue(password)) {
          final var s = PasswordUtil.decrypt(password.toString());
          passwordSupplier = () -> s;
        } else {
          passwordSupplier = null;
        }
      }
      return new JdbcDataSourceImpl.Builder()//
        .setDriverClassName(getDriverClassName())
        .setUrl(url)
        .setMaxIdle(maxIdle)
        .setMaxPoolSize(maxPoolSize)
        .setMaxWait(maxWait)
        .setMaxAge(maxAge)
        .setUserSupplier(userSupplier)
        .setPasswordSupplier(passwordSupplier)
        .setConfig(newConfig)
        .build();
    } catch (final Throwable e) {
      throw new IllegalArgumentException("Unable to create data source for " + config, e);
    }
  }

  default String newMessage(final String task, final String sql, final SQLException exception) {
    return task + "; " + (sql != null ? "SQL [" + sql + "]; " : "") + exception.getMessage();
  }

  JdbcRecordStore newRecordStore(DataSource dataSource);

  @Override
  JdbcRecordStore newRecordStore(MapEx connectionProperties);

  default JdbcRecordStore newRecordStoreInitialized(final MapEx connectionProperties) {
    final var recordStore = newRecordStore(connectionProperties);
    if (recordStore != null) {
      recordStore.initialize();
    }
    return recordStore;
  }

  default Duration removeDuration(final MapEx config, final String key,
    final Duration defaultValue) {
    final var value = config.remove(key);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof final Duration duration) {
      return duration;
    } else if (value instanceof final Number number) {
      return Duration.ofSeconds(number.longValue());
    } else {
      throw new IllegalArgumentException(
        key + " must be a number or a Duration not " + value.getClass());
    }
  }

  default int removeInteger(final MapEx newConfig, final String key, final int defaultValue) {
    final int value = newConfig.getInteger(key, defaultValue);
    newConfig.remove(key);
    return value;
  }

  DataAccessException translateException(String message, String sql, SQLException exception);
}
