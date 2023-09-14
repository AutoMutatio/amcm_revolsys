package com.revolsys.jdbc;

import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Arrays;
import java.util.Enumeration;

import javax.management.ObjectName;
import javax.sql.DataSource;

import org.jeometry.common.exception.Exceptions;
import org.jeometry.common.io.PathName;
import org.jeometry.common.logging.Logs;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.SQLExceptionTranslator;
import org.springframework.jdbc.support.SQLStateSQLExceptionTranslator;

import com.revolsys.collection.map.MapEx;
import com.revolsys.io.PathUtil;
import com.revolsys.jdbc.exception.JdbcExceptionTranslator;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcFieldDefinitions;
import com.revolsys.record.io.format.json.JsonObject;
import com.revolsys.util.Property;
import com.revolsys.util.Strings;

public final class JdbcUtils {

  public static void close(final ResultSet resultSet) {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (final SQLException e) {
        Logs.debug(JdbcUtils.class, "SQL error closing result set", e);
      } catch (final Throwable e) {
        Logs.debug(JdbcUtils.class, "Unknown error closing result set", e);
      }
    }
  }

  public static void close(final Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (final SQLException e) {
        Logs.debug(JdbcUtils.class, "SQL error closing statement", e);
      } catch (final Throwable e) {
        Logs.debug(JdbcUtils.class, "Unknown error closing statement", e);
      }
    }
  }

  public static void close(final Statement statement, final ResultSet resultSet) {
    close(resultSet);
    close(statement);
  }

  public static int executeUpdate(final Connection connection, final String sql,
    final Object... parameters) {
    try {
      final PreparedStatement statement = connection.prepareStatement(sql);
      try {
        JdbcUtils.setParameters(statement, parameters);
        return statement.executeUpdate();
      } finally {
        JdbcUtils.close(statement);
      }
    } catch (final SQLException e) {
      throw Exceptions.wrap("Update:\n" + sql, e);
    }
  }

  public static Connection getConnection(final DataSource dataSource) {
    try {
      Connection connection = DataSourceUtils.doGetConnection(dataSource);
      if (connection.isClosed()) {
        DataSourceUtils.doReleaseConnection(connection, dataSource);
        connection = DataSourceUtils.doGetConnection(dataSource);
      }

      return connection;
    } catch (final SQLException e) {
      throw getException(dataSource, "Get Connection", null, e);
    }
  }

  public static RuntimeException getException(final DataSource dataSource, final String task,
    final String sql, final SQLException e) {
    SQLExceptionTranslator translator;
    if (dataSource == null) {
      translator = new SQLStateSQLExceptionTranslator();
    } else {
      translator = new JdbcExceptionTranslator(dataSource);
    }
    final DataAccessException exception = translator.translate(task, sql, e);
    if (exception == null) {
      return Exceptions.wrap(Strings.toString("\n", task, sql), e);
    }
    return exception;
  }

  public static String getProductName(final DataSource dataSource) {
    if (dataSource == null) {
      return null;
    } else {
      final Connection connection = getConnection(dataSource);
      try {
        if (connection == null) {
          if (dataSource.getClass().getName().toLowerCase().contains("oracle")) {
            return "Oracle";
          } else if (dataSource.getClass().getName().toLowerCase().contains("postgres")) {
            return "PostgreSQL";
          } else {
            return null;
          }
        } else {
          final DatabaseMetaData metaData = connection.getMetaData();
          return metaData.getDatabaseProductName();
        }
      } catch (final SQLException e) {
        throw new IllegalArgumentException("Unable to get database product name", e);
      } finally {
        release(connection, dataSource);
      }
    }
  }

  public static String getQualifiedTableName(final PathName pathName) {
    if (Property.hasValue(pathName)) {
      final String path = pathName.toString();
      return getQualifiedTableName(path);
    } else {
      return null;
    }
  }

  public static String getQualifiedTableName(final String typePath) {
    if (Property.hasValue(typePath)) {
      final String tableName = typePath.replaceAll("^/+", "");
      return tableName.replaceAll("/", ".");
    } else {
      return null;
    }
  }

  public static String getSchemaName(final String typePath) {
    if (Property.hasValue(typePath)) {
      final String path = PathUtil.getPath(typePath);
      return path.replaceAll("(^/|/$)", "");
    } else {
      return "";
    }
  }

  public static void lockTable(final Connection connection, final String tableName)
    throws SQLException {
    final String sql = "LOCK TABLE " + tableName + " IN SHARE MODE";
    final PreparedStatement statement = connection.prepareStatement(sql);
    try {
      statement.execute();
    } finally {
      close(statement);
    }
  }

  public static MapEx readMap(final ResultSet rs) throws SQLException {
    final MapEx values = JsonObject.hash();
    final ResultSetMetaData metaData = rs.getMetaData();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      final String name = metaData.getColumnName(i);
      final Object value = rs.getObject(i);
      values.put(name, value);
    }
    return values;
  }

  public static void release(final Connection connection, final DataSource dataSource) {
    if (dataSource != null && connection != null) {
      DataSourceUtils.releaseConnection(connection, dataSource);
    }
  }

  public static MapEx selectMap(final Connection connection, final String sql,
    final Object... parameters) throws SQLException {
    final PreparedStatement statement = connection.prepareStatement(sql);
    try {
      setParameters(statement, parameters);
      final ResultSet resultSet = statement.executeQuery();
      try {
        if (resultSet.next()) {
          return readMap(resultSet);
        } else {
          throw new IllegalArgumentException(
            "Value not found for " + sql + " " + Arrays.asList(parameters));
        }
      } finally {
        close(resultSet);
      }
    } finally {
      close(statement);
    }
  }

  public static void setParameters(final PreparedStatement statement, final Object... parameters)
    throws SQLException {
    int index = 1;
    for (final Object parameter : parameters) {
      index = setValue(statement, index, parameter);
    }
  }

  public static int setValue(final PreparedStatement statement, final int index, final Object value)
    throws SQLException {
    final JdbcFieldDefinition fieldDefinition = JdbcFieldDefinitions.newFieldDefinition(value);
    return fieldDefinition.setPreparedStatementValue(statement, index, value);
  }

  public static Struct struct(final Connection connection, final String type, final Object... args)
    throws SQLException {
    return connection.createStruct(type, args);
  }

  public static void unregisterDrivers(final ClassLoader classLoader) {
    try {
      final Enumeration<Driver> drivers = DriverManager.getDrivers();
      while (drivers.hasMoreElements()) {
        final Driver driver = drivers.nextElement();
        try {
          final Class<? extends Driver> driverClass = driver.getClass();
          if (driverClass.getClassLoader() == classLoader) {
            DriverManager.deregisterDriver(driver);
          }

        } catch (final Throwable e) {
          Logs.error(JdbcUtils.class, "Unable to unregister driver: " + driver, e);
        }
      }
    } catch (final Throwable e) {
      Logs.error(JdbcUtils.class, "Unable to unregister drivers", e);
    }
    try {
      // Cleanup Oracle MBean
      final ObjectName objectname = new ObjectName("com.oracle.jdbc:type=diagnosability,name="
        + classLoader.getClass().getName() + "@" + Integer.toHexString(classLoader.hashCode()));
      ManagementFactory.getPlatformMBeanServer().unregisterMBean(objectname);
    } catch (final Throwable e) {
      Logs.error(JdbcUtils.class, "Unable to remove Oracle MBean", e);
    }
  }

  private JdbcUtils() {

  }
}
