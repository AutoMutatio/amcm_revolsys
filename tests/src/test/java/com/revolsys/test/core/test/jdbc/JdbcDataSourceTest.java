package com.revolsys.test.core.test.jdbc;

import java.sql.SQLException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.jupiter.api.Test;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.exception.Exceptions;
import com.revolsys.jdbc.JdbcDataSource;
import com.revolsys.jdbc.JdbcDatabaseFactory;
import com.revolsys.logging.Logs;
import com.revolsys.util.concurrent.Concurrent;

public class JdbcDataSourceTest {

  private JdbcDataSource newDataSource() {
    final var dataSource = JdbcDatabaseFactory.dataSource(JsonObject.hash()//
      .addValue("url", "jdbc:postgresql://localhost:42032/AmCmDevelopment")
      .addValue("user", "AmCmDevelopment")
      .addValue("password", "12345678")
      .addValue("maxPoolSize", 2)
      .addValue("maxWait", Duration.ofSeconds(5)
        .plus(5, ChronoUnit.MILLIS))
      .addValue("maxAge", Duration.ofSeconds(4)));
    return dataSource;
  }

  private void runError(final JdbcDataSource dataSource) {
    try (
      var connection = dataSource.getConnection()) {
      connection.createStatement()
        .execute("select error");
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  private void runTest(final JdbcDataSource dataSource) {
    try (
      var connection = dataSource.getConnection()) {
      connection.createStatement()
        .execute("select now()");
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Test
  public void testDataSourceConnection() {
    final var dataSource = newDataSource();
    Concurrent.virtual()
      .forEach(v -> {
        for (int i = 0; i < 1000; i++) {
          final boolean error = i % 13 == 0;
          try {
            if (error) {
              runError(dataSource);
            } else {
              runTest(dataSource);
            }
            LockSupport.parkNanos(Duration.ofMillis(20)
              .toNanos());
          } catch (final Exception e) {
            if (!error || !e.getMessage()
              .contains("ERROR: column \"error\" does not exist")) {
              Logs.error(this, e);
            }
          }
        }
      }, 1, 2, 3, 4, 5);
  }

  @Test
  public void testDataSourceTransaction() {
    final var dataSource = newDataSource();
    Concurrent.virtual()
      .forEach(v -> {
        for (int i = 0; i < 1000; i++) {
          final boolean error = i % 13 == 0;
          try {
            dataSource.transactionNewRun(() -> {
              if (error) {
                runError(dataSource);
              } else {
                runTest(dataSource);
              }
            });
            LockSupport.parkNanos(Duration.ofMillis(20)
              .toNanos());
          } catch (final Exception e) {
            if (!error || !e.getMessage()
              .contains("ERROR: column \"error\" does not exist")) {
              Logs.error(this, e);
            }
          }
        }
      }, 1, 2, 3, 4, 5);
  }
}
