package com.revolsys.snowflake;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.io.AbstractJdbcDatabaseFactory;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;
import com.revolsys.jdbc.io.JdbcRecordStoreSchema;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;

public class SnowflakeRecordStore extends AbstractJdbcRecordStore {

  public static final List<String> INTERNAL_SCHEMAS = Arrays.asList("INFORMATION_SCHEMA");

  public SnowflakeRecordStore(final AbstractJdbcDatabaseFactory databaseFactory,
    final Map<String, ? extends Object> connectionProperties) {
    super(databaseFactory, connectionProperties);
    initSettings();
  }

  public SnowflakeRecordStore(final DataSource dataSource) {
    super(dataSource);
    initSettings();
  }

  @Override
  public boolean exists(final Query query) {
    final var existQuery = newQuery().select(query.asExists());
    final String sql = getSelectSql(existQuery);
    try (
      var connection = getJdbcConnection()) {
      try (
        var statement = connection.prepareStatement(sql);
        var resultSet = getResultSet(statement, query)) {
        if (resultSet.next()) {
          return resultSet.getBoolean(1);
        } else {
          return false;
        }
      }

    } catch (final SQLException e) {
      throw getException("Execute Query", sql, e);
    }
  }

  @Override
  public String getCatalogueName() {
    return getConnectionProperties().getString("db");
  }

  @Override
  public String getRecordStoreType() {
    return "Snowflake";
  }

  @Override
  protected String getSelectSql(final Query query) {
    String sql = super.getSelectSql(query);
    final int offset = query.getOffset();
    final int limit = query.getLimit();
    if (offset > 0 || limit != Integer.MAX_VALUE) {
      sql += " LIMIT " + limit;
      sql += " OFFSET " + offset;
    }
    return sql;
  }

  @Override
  public void initializeDo() {
    super.initializeDo();
  }

  protected void initSettings() {
    this.ignoreCatalogues.add("SNOWFLAKE");
    this.ignoreSchemas.add("INFORMATION_SCHEMA");
    setQuoteNames(true);
  }

  @Override
  public PreparedStatement insertStatementPrepareRowId(final JdbcConnection connection,
    final RecordDefinition recordDefinition, final String sql) throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isSchemaExcluded(final String schemaName) {
    return INTERNAL_SCHEMAS.contains(schemaName);
  }

  @Override
  protected JdbcRecordDefinition newRecordDefinition(final JdbcRecordStoreSchema schema,
    final PathName pathName, final String dbTableName) {
    final String quotedTableName = '"' + dbTableName + '"';
    return super.newRecordDefinition(schema, pathName, quotedTableName);
  }

  @Override
  protected SnowflakeRecordStoreSchema newRootSchema() {
    return new SnowflakeRecordStoreSchema(this);
  }

  @Override
  protected SnowflakeRecordStoreSchema newSchema(final JdbcRecordStoreSchema rootSchema,
    final String dbSchemaName, final PathName childSchemaPath) {
    final boolean quoteName = !dbSchemaName.equals(dbSchemaName.toLowerCase());
    return new SnowflakeRecordStoreSchema((SnowflakeRecordStoreSchema)rootSchema, childSchemaPath,
      dbSchemaName, quoteName);
  }

}
