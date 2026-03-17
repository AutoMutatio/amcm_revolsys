package com.revolsys.mssqlserver;

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
import com.revolsys.jdbc.io.JdbcRecordStoreSchema;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.query.Query;
import com.revolsys.record.schema.RecordDefinition;

public class MicrosoftSqlServerRecordStore extends AbstractJdbcRecordStore {

  public static final List<String> INTERNAL_SCHEMAS = Arrays.asList("guest", "INFORMATION_SCHEMA",
    "sys", "db_owner", "db_accessadmin", "db_securityadmin", "db_ddladmin", "db_backupoperator",
    "db_datareader", "db_datawriter", "db_denydatareader", "db_denydatawriter");

  public MicrosoftSqlServerRecordStore() {
    this(ArrayRecord.FACTORY);
  }

  public MicrosoftSqlServerRecordStore(final AbstractJdbcDatabaseFactory databaseFactory,
    final Map<String, ? extends Object> connectionProperties) {
    super(databaseFactory, connectionProperties);
    initSettings();
  }

  public MicrosoftSqlServerRecordStore(final DataSource dataSource) {
    super(dataSource);
    initSettings();
  }

  public MicrosoftSqlServerRecordStore(final RecordFactory<? extends Record> recordFactory) {
    super(recordFactory);
    initSettings();
  }

  public MicrosoftSqlServerRecordStore(final RecordFactory<? extends Record> recordFactory,
    final DataSource dataSource) {
    this(recordFactory);
    setDataSource(dataSource);
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
  public String getRecordStoreType() {
    return "MicrosoftSqlServer";
  }

  @Override
  public void initializeDo() {
    super.initializeDo();
    // final JdbcFieldAdder numberFieldAdder = new
    // JdbcFieldAdder(DataTypes.DECIMAL);
    // addFieldAdder("numeric", numberFieldAdder);
    //
    // final JdbcStringFieldAdder stringFieldAdder = new JdbcStringFieldAdder();
    // addFieldAdder("varchar", stringFieldAdder);
    // addFieldAdder("text", stringFieldAdder);
    // addFieldAdder("name", stringFieldAdder);
    // addFieldAdder("bpchar", stringFieldAdder);
    //
    // final JdbcFieldAdder longFieldAdder = new JdbcFieldAdder(DataTypes.LONG);
    // addFieldAdder("int8", longFieldAdder);
    // addFieldAdder("bigint", longFieldAdder);
    // addFieldAdder("bigserial", longFieldAdder);
    // addFieldAdder("serial8", longFieldAdder);
    //
    // final JdbcFieldAdder intFieldAdder = new JdbcFieldAdder(DataTypes.INT);
    // addFieldAdder("int4", intFieldAdder);
    // addFieldAdder("integer", intFieldAdder);
    // addFieldAdder("serial", intFieldAdder);
    // addFieldAdder("serial4", intFieldAdder);
    //
    // final JdbcFieldAdder shortFieldAdder = new
    // JdbcFieldAdder(DataTypes.SHORT);
    // addFieldAdder("int2", shortFieldAdder);
    // addFieldAdder("smallint", shortFieldAdder);
    //
    // final JdbcFieldAdder floatFieldAdder = new
    // JdbcFieldAdder(DataTypes.FLOAT);
    // addFieldAdder("float4", floatFieldAdder);
    //
    // final JdbcFieldAdder doubleFieldAdder = new
    // JdbcFieldAdder(DataTypes.DOUBLE);
    // addFieldAdder("float8", doubleFieldAdder);
    // addFieldAdder("double precision", doubleFieldAdder);
    //
    // addFieldAdder("date", new JdbcFieldAdder(DataTypes.DATE_TIME));
    // addFieldAdder("timestamp", new JdbcFieldAdder(DataTypes.TIMESTAMP));
    // addFieldAdder("timestamptz", new JdbcFieldAdder(DataTypes.TIMESTAMP));
    //
    // addFieldAdder("interval", PostgreSQLJdbcIntevalFieldDefinition::new);
    //
    // addFieldAdder("bool", new JdbcFieldAdder(DataTypes.BOOLEAN));
    //
    // addFieldAdder("citext", PostgreSQLCiTextFieldDefinition::new);
    //
    // addFieldAdder("uuid", new JdbcFieldAdder(DataTypes.UUID));
    //
    // addFieldAdder("oid", PostgreSQLJdbcBlobFieldDefinition::new);
    //
    // addFieldAdder("jsonb", PostgreSQLJsonbFieldDefinition::new);
    //
    // addFieldAdder(Types.STRUCT, PostgreSQLStructFieldDefinition::new);
    //
    // final JdbcFieldAdder geometryFieldAdder = new
    // PostgreSQLGeometryFieldAdder(this);
    // addFieldAdder("geometry", geometryFieldAdder);
    setPrimaryKeySql("""
SELECT
  t.name "TABLE_NAME",
  c.name "COLUMN_NAME"
FROM
  sys.schemas s
  JOIN sys.tables t ON t.schema_id = s.schema_id
  JOIN sys.indexes i ON i.object_id = t.object_id
  JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
  JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
WHERE
  i.is_primary_key = 1 AND
  s.name = ?
order by
  t.name,
  ic.key_ordinal
    """);
    setSchemaPermissionsSql("""
select distinct
  name as "SCHEMA_NAME"
from
  sys.schemas
WHERE
  name NOT in (
    'guest',
    'INFORMATION_SCHEMA',
    'sys',
    'db_owner',
    'db_accessadmin',
    'db_securityadmin',
    'db_ddladmin',
    'db_backupoperator',
    'db_datareader',
    'db_datawriter',
    'db_denydatareader',
    'db_denydatawriter'
  )
    """);
    setSchemaTablePermissionsSql("""
select
  distinct
  s.name as "SCHEMA_NAME",
  t.name as "TABLE_NAME",
  tp.permission_name as "PRIVILEGE",
  td.value as "REMARKS",
  CASE
    WHEN t.type_desc = 'USER_TABLE' THEN 'TABLE'
    ELSE t.type_desc
  END "TABLE_TYPE"
from
  sys.tables t
    join sys.schemas s on s.schema_id = t.schema_id
    left join sys.extended_properties td on td.major_id = t.object_id  and td.name = 'MS_Description' and td.minor_id = 0
    CROSS APPLY sys.fn_my_permissions(s.name || '.' || t.name, 'OBJECT') tp
where
  s.name = ? AND
  tp.permission_name IN (
    'SELECT', 'INSERT', 'UPDATE', 'DELETE'
  )
order by
  s.name,
  t.name,
  tp.permission_name
    """);
  }

  protected void initSettings() {
    setExcludeTablePaths();
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
  protected MicrosoftSqlServerRecordStoreSchema newRootSchema() {
    return new MicrosoftSqlServerRecordStoreSchema(this);
  }

  @Override
  protected MicrosoftSqlServerRecordStoreSchema newSchema(final JdbcRecordStoreSchema rootSchema,
    final String dbSchemaName, final PathName childSchemaPath) {
    final boolean quoteName = !dbSchemaName.equals(dbSchemaName.toLowerCase());
    return new MicrosoftSqlServerRecordStoreSchema((MicrosoftSqlServerRecordStoreSchema)rootSchema,
      childSchemaPath, dbSchemaName, quoteName);
  }

}
