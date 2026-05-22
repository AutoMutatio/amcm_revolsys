package com.revolsys.snowflake;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.springframework.dao.DataAccessException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.jdbc.io.AbstractJdbcDatabaseFactory;
import com.revolsys.jdbc.io.JdbcRecordStore;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordStore;

import net.snowflake.client.api.driver.SnowflakeDriver;

// Not snowflake uses arrow, need to add the following JVM arg
// --add-opens java.base/java.nio=ALL-UNNAMED
// To stop the annoying System.err.println
// -Dsun.misc.unsafe.memory.access=allow
public class Snowflake extends AbstractJdbcDatabaseFactory {

  private static final List<FieldDefinition> CONNECTION_FIELD_DEFINITIONS = Arrays.asList();

  public Snowflake() {
  }

  @Override
  public List<FieldDefinition> getConnectionFieldDefinitions() {
    return CONNECTION_FIELD_DEFINITIONS;
  }

  @Override
  public String getDriverClassName() {
    return SnowflakeDriver.class.getName();
  }

  @Override
  public String getName() {
    return "Snowflake Database";
  }

  @Override
  public String getProductName() {
    // TODO is this correct
    return "Snowflake JDBC Driver";
  }

  @Override
  public List<String> getRecordStoreFileExtensions() {
    return Collections.emptyList();
  }

  @Override
  public Class<? extends RecordStore> getRecordStoreInterfaceClass(
    final Map<String, ? extends Object> connectionProperties) {
    return JdbcRecordStore.class;
  }

  @Override
  public String getVendorName() {
    return "snowflake";
  }

  @Override
  public JdbcRecordStore newRecordStore(final DataSource dataSource) {
    return new SnowflakeRecordStore(dataSource);
  }

  @Override
  public JdbcRecordStore newRecordStore(final MapEx connectionProperties) {
    return new SnowflakeRecordStore(this, connectionProperties);
  }

  @Override
  public String toString() {
    return getName();
  }

  @Override
  public DataAccessException translateException(final String task, final String sql,
    final SQLException exception) {
    return translateSqlStateException(task, sql, exception);
  }
}
