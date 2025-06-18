package com.revolsys.gis.postgresql;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.postgresql.jdbc.PgConnection;

import com.revolsys.collection.set.Sets;
import com.revolsys.data.identifier.Identifier;
import com.revolsys.data.type.CollectionDataType;
import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.exception.Exceptions;
import com.revolsys.gis.postgresql.type.PostgreSQLArrayFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLBoundingBoxWrapper;
import com.revolsys.gis.postgresql.type.PostgreSQLGeometryFieldAdder;
import com.revolsys.gis.postgresql.type.PostgreSQLGeometryWrapper;
import com.revolsys.gis.postgresql.type.PostgreSQLJdbcBlobFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLJdbcEnumFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLJdbcIntevalFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLJsonbFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLOidFieldDefinition;
import com.revolsys.gis.postgresql.type.PostgreSQLTidWrapper;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcConnection;
import com.revolsys.jdbc.field.JdbcFieldAdder;
import com.revolsys.jdbc.field.JdbcFieldDefinition;
import com.revolsys.jdbc.field.JdbcStringFieldAdder;
import com.revolsys.jdbc.io.AbstractJdbcDatabaseFactory;
import com.revolsys.jdbc.io.AbstractJdbcRecordStore;
import com.revolsys.jdbc.io.JdbcRecordDefinition;
import com.revolsys.jdbc.io.JdbcRecordStoreSchema;
import com.revolsys.record.ArrayRecord;
import com.revolsys.record.Record;
import com.revolsys.record.RecordFactory;
import com.revolsys.record.property.ShortNameProperty;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.functions.EnvelopeIntersects;
import com.revolsys.record.query.functions.JsonValue;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStoreSchemaElement;
import com.revolsys.util.Property;

public class PostgreSQLRecordStore extends AbstractJdbcRecordStore {

  public static final List<String> POSTGRESQL_INTERNAL_SCHEMAS = Arrays.asList("information_schema",
    "pg_catalog", "pg_toast_temp_1");

  private boolean useSchemaSequencePrefix = true;

  public PostgreSQLRecordStore() {
    this(ArrayRecord.FACTORY);
  }

  public PostgreSQLRecordStore(final AbstractJdbcDatabaseFactory databaseFactory,
    final Map<String, ? extends Object> connectionProperties) {
    super(databaseFactory, connectionProperties);
    initSettings();
  }

  public PostgreSQLRecordStore(final DataSource dataSource) {
    super(dataSource);
    initSettings();
  }

  public PostgreSQLRecordStore(final RecordFactory<? extends Record> recordFactory) {
    super(recordFactory);
    initSettings();
  }

  public PostgreSQLRecordStore(final RecordFactory<? extends Record> recordFactory,
    final DataSource dataSource) {
    this(recordFactory);
    setDataSource(dataSource);
  }

  @Override
  protected JdbcFieldDefinition addField(final JdbcRecordDefinition recordDefinition,
    final String dbColumnName, final String name, final int sqlType, final String dbDataType,
    final int length, final int scale, final boolean required, final String description) {
    final var dbType = PathName.fromDotSeparated(dbDataType.replaceAll("\"", ""));
    final JdbcFieldDefinition field;
    if (((JdbcRecordStoreSchema)recordDefinition.getSchema()).isEnum(dbType)) {
      field = new PostgreSQLJdbcEnumFieldDefinition(dbColumnName, name, sqlType, dbDataType, length,
        scale, required, description, getProperties());
      field.setQuoteName(isQuoteNames());
      recordDefinition.addField(field);
    } else if (dbDataType.charAt(0) == '_') {
      final String elementDbDataType = dbDataType.substring(1);
      final JdbcFieldAdder fieldAdder = getFieldAdder(elementDbDataType);
      final JdbcFieldDefinition elementField = fieldAdder.newField(this, recordDefinition,
        dbColumnName, name, sqlType, elementDbDataType, length, scale, required, description);

      final DataType elementDataType = elementField.getDataType();
      final CollectionDataType listDataType = new CollectionDataType(
        "List" + elementDataType.getName(), List.class, elementDataType);
      field = new PostgreSQLArrayFieldDefinition(dbColumnName, name, listDataType,
        elementDbDataType, sqlType, dbDataType, length, scale, required, description, elementField,
        getProperties());
      field.setQuoteName(isQuoteNames());
      recordDefinition.addField(field);
    } else {
      field = super.addField(recordDefinition, dbColumnName, name, sqlType, dbDataType, length,
        scale, required, description);
    }
    if (!dbColumnName.equals(dbColumnName.toLowerCase())) {
      field.setQuoteName(true);
    }
    return field;
  }

  private void appendEnvelopeIntersects(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final EnvelopeIntersects envelopeIntersects = (EnvelopeIntersects)queryValue;
    final QueryValue boundingBox1Value = envelopeIntersects.getBoundingBox1Value();
    if (boundingBox1Value == null) {
      sql.append("NULL");
    } else {
      appendQueryValue(statement, sql, boundingBox1Value);
    }
    sql.append(" && ");
    final QueryValue boundingBox2Value = envelopeIntersects.getBoundingBox2Value();
    if (boundingBox2Value == null) {
      sql.append("NULL");
    } else {
      appendQueryValue(statement, sql, boundingBox2Value);
    }
  }

  private void appendJsonValue(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final JsonValue jsonValue = (JsonValue)queryValue;
    final QueryValue jsonParameter = jsonValue.getParameter(0);
    sql.append('(');
    jsonParameter.appendSql(statement, this, sql);

    final String[] path = jsonValue.getPath()
      .split("\\.");
    for (int i = 1; i < path.length; i++) {
      final String propertyName = path[i];
      if (jsonValue.isText()) {
        sql.append(" ->> '");
      } else {
        sql.append(" -> '");
      }
      sql.append(propertyName);
      sql.append("'");
    }
    sql.append(")");
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
  public String getGeneratePrimaryKeySql(final JdbcRecordDefinition recordDefinition) {
    final String sequenceName = getSequenceName(recordDefinition);
    return "nextval('" + sequenceName + "')";
  }

  @Override
  protected Identifier getNextPrimaryKey(final String sequenceName) {
    final String sql = "SELECT nextval(?)";
    return Identifier.newIdentifier(selectLong(sql, sequenceName));
  }

  @Override
  public String getRecordStoreType() {
    return "PostgreSQL";
  }

  @Override
  protected String getSelectSql(final Query query) {
    String sql = super.getSelectSql(query);
    final int offset = query.getOffset();
    if (offset > 0) {
      sql += " OFFSET " + offset;
    }
    final int limit = query.getLimit();
    if (limit != Integer.MAX_VALUE) {
      sql += " LIMIT " + limit;
    }
    return sql;
  }

  @Override
  protected String getSequenceName(final JdbcRecordDefinition recordDefinition) {
    final JdbcRecordStoreSchema schema = recordDefinition.getSchema();
    final String dbSchemaName = schema.getQuotedDbName();
    final String shortName = ShortNameProperty.getShortName(recordDefinition);
    String sequenceName;
    if (Property.hasValue(shortName)) {
      if (this.useSchemaSequencePrefix) {
        sequenceName = dbSchemaName + "." + shortName.toLowerCase() + "_seq";
      } else {
        sequenceName = shortName.toLowerCase() + "_seq";
      }
    } else {
      final String tableName = recordDefinition.getDbTableName();
      final String idFieldName = ((JdbcFieldDefinition)recordDefinition.getIdField()).getDbName();
      sequenceName = '"' + tableName.replace("\"", "") + "_" + idFieldName + "_seq\"";
      if (this.useSchemaSequencePrefix) {
        return dbSchemaName + "." + sequenceName;
      }
    }
    return sequenceName;

  }

  @Override
  protected void initConnection(final Connection connection) {
    super.initConnection(connection);
    try {
      final PgConnection pgConnection = connection.unwrap(PgConnection.class);
      pgConnection.addDataType("geometry", PostgreSQLGeometryWrapper.class);
      pgConnection.addDataType("box2d", PostgreSQLBoundingBoxWrapper.class);
      pgConnection.addDataType("box3d", PostgreSQLBoundingBoxWrapper.class);
      pgConnection.addDataType("tid", PostgreSQLTidWrapper.class);
    } catch (final SQLException e) {
      throw Exceptions.wrap("Unable to initialize connection", e);
    }
  }

  @Override
  public void initializeDo() {
    super.initializeDo();
    final JdbcFieldAdder numberFieldAdder = new JdbcFieldAdder(DataTypes.DECIMAL);
    addFieldAdder("numeric", numberFieldAdder);

    final JdbcStringFieldAdder stringFieldAdder = new JdbcStringFieldAdder();
    addFieldAdder("varchar", stringFieldAdder);
    addFieldAdder("text", stringFieldAdder);
    addFieldAdder("citext", stringFieldAdder);
    addFieldAdder("name", stringFieldAdder);
    addFieldAdder("bpchar", stringFieldAdder);

    final JdbcFieldAdder longFieldAdder = new JdbcFieldAdder(DataTypes.LONG);
    addFieldAdder("int8", longFieldAdder);
    addFieldAdder("bigint", longFieldAdder);
    addFieldAdder("bigserial", longFieldAdder);
    addFieldAdder("serial8", longFieldAdder);

    final JdbcFieldAdder intFieldAdder = new JdbcFieldAdder(DataTypes.INT);
    addFieldAdder("int4", intFieldAdder);
    addFieldAdder("integer", intFieldAdder);
    addFieldAdder("serial", intFieldAdder);
    addFieldAdder("serial4", intFieldAdder);

    final JdbcFieldAdder shortFieldAdder = new JdbcFieldAdder(DataTypes.SHORT);
    addFieldAdder("int2", shortFieldAdder);
    addFieldAdder("smallint", shortFieldAdder);

    final JdbcFieldAdder floatFieldAdder = new JdbcFieldAdder(DataTypes.FLOAT);
    addFieldAdder("float4", floatFieldAdder);

    final JdbcFieldAdder doubleFieldAdder = new JdbcFieldAdder(DataTypes.DOUBLE);
    addFieldAdder("float8", doubleFieldAdder);
    addFieldAdder("double precision", doubleFieldAdder);

    addFieldAdder("date", new JdbcFieldAdder(DataTypes.DATE_TIME));
    addFieldAdder("timestamp", new JdbcFieldAdder(DataTypes.TIMESTAMP));
    addFieldAdder("timestamptz", new JdbcFieldAdder(DataTypes.TIMESTAMP));

    addFieldAdder("interval", PostgreSQLJdbcIntevalFieldDefinition::new);

    addFieldAdder("bool", new JdbcFieldAdder(DataTypes.BOOLEAN));

    addFieldAdder("uuid", new JdbcFieldAdder(DataTypes.UUID));

    addFieldAdder("oid", PostgreSQLJdbcBlobFieldDefinition::new);

    addFieldAdder("jsonb", PostgreSQLJsonbFieldDefinition::new);

    final JdbcFieldAdder geometryFieldAdder = new PostgreSQLGeometryFieldAdder(this);
    addFieldAdder("geometry", geometryFieldAdder);
    setPrimaryKeySql("SELECT t.relname \"TABLE_NAME\", c.attname \"COLUMN_NAME\"" //
      + " FROM pg_namespace s" //
      + " join pg_class t on t.relnamespace = s.oid" //
      + " join pg_index i on i.indrelid = t.oid " //
      + " join pg_attribute c on c.attrelid = t.oid" //
      + " WHERE s.nspname = ? AND c.attnum = any(i.indkey) AND i.indisprimary");
    setPrimaryKeyTableCondition(" AND r.relname = ?");
    setSchemaPermissionsSql("select distinct t.table_schema as \"SCHEMA_NAME\" "
      + "from information_schema.role_table_grants t  "
      + "where (t.grantee  in (current_user, 'PUBLIC') or "
      + "t.grantee in (select role_name from information_schema.applicable_roles r where r.grantee = current_user)) and "
      + "privilege_type IN ('SELECT', 'INSERT','UPDATE','DELETE') ");
    setSchemaTablePermissionsSql(
      "select distinct t.table_schema as \"SCHEMA_NAME\", t.table_name, t.privilege_type as \"PRIVILEGE\", d.description as \"REMARKS\", "
        + "  CASE WHEN relkind = 'r' THEN 'TABLE' WHEN relkind = 'v' THEN 'VIEW' ELSE relkind::text || '' END \"TABLE_TYPE\" "
        + "from" //
        + "  information_schema.role_table_grants t"//
        + "    join pg_namespace n on t.table_schema = n.nspname"//
        + "    join pg_class c on (n.oid = c.relnamespace AND t.table_name = c.relname)"//
        + "    left join pg_description d on d.objoid = c.oid "//
        + "where" //
        + "  t.table_schema = ? and "//
        + "  (t.grantee in (current_user, 'PUBLIC') or t.grantee in (select role_name from information_schema.applicable_roles r where r.grantee = current_user)) AND "
        + "  privilege_type IN ('SELECT', 'INSERT','UPDATE','DELETE') "
        + "  order by t.table_schema, t.table_name, t.privilege_type");
  }

  protected void initSettings() {
    setExcludeTablePaths("/PUBLIC/GEOMETRY_COLUMNS", "/PUBLIC/GEOGRAPHY_COLUMNS",
      "/PUBLIC/PG_BUFFER_CACHE", "/PUBLIC/PG_STAT_STATEMENTS", "/PUBLIC/SPATIAL_REF_SYS");
    addSqlQueryAppender(EnvelopeIntersects.class, this::appendEnvelopeIntersects);
    addSqlQueryAppender(JsonValue.class, this::appendJsonValue);
  }

  @Override
  public PreparedStatement insertStatementPrepareRowId(final JdbcConnection connection,
    final RecordDefinition recordDefinition, final String sql) throws SQLException {
    String[] generatedColumnNames = recordDefinition.getProperty("generatedColumnNames");
    if (generatedColumnNames == null) {
      final List<FieldDefinition> idFields = recordDefinition.getIdFields();
      final Set<FieldDefinition> generatedFields = new LinkedHashSet<>();
      generatedFields.addAll(idFields);
      for (final FieldDefinition field : recordDefinition.getFields()) {
        if (field.isGenerated()) {
          generatedFields.add(field);
        }
      }
      generatedColumnNames = new String[generatedFields.size()];
      int i = 0;
      for (final FieldDefinition generatedField : generatedFields) {
        generatedColumnNames[i++] = ((JdbcFieldDefinition)generatedField).getDbName();
      }
      recordDefinition.setProperty("generatedColumnNames", generatedColumnNames);
    }
    return connection.prepareStatement(sql, generatedColumnNames);
  }

  @Override
  public boolean isIdFieldRowid(final RecordDefinition recordDefinition) {
    final List<FieldDefinition> idFields = recordDefinition.getIdFields();
    if (idFields.size() == 1) {
      final FieldDefinition idField = idFields.get(0);
      if (idField instanceof PostgreSQLOidFieldDefinition) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isSchemaExcluded(final String schemaName) {
    return POSTGRESQL_INTERNAL_SCHEMAS.contains(schemaName);
  }

  public boolean isUseSchemaSequencePrefix() {
    return this.useSchemaSequencePrefix;
  }

  @Override
  public Array newArray(final Connection connection, final String typeName, final Object array) {
    try {
      return connection.unwrap(PgConnection.class)
        .createArrayOf(typeName, array);
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
  }

  @Override
  protected JdbcRecordDefinition newRecordDefinition(final JdbcRecordStoreSchema schema,
    final PathName pathName, String dbTableName) {
    if (dbTableName != null && dbTableName.charAt(0) != '"'
      && !dbTableName.equals(dbTableName.toLowerCase())) {
      dbTableName = '"' + dbTableName + '"';
    }
    return super.newRecordDefinition(schema, pathName, dbTableName);
  }

  @Override
  protected PostgreSQLRecordStoreSchema newRootSchema() {
    return new PostgreSQLRecordStoreSchema(this);
  }

  @Override
  protected JdbcFieldDefinition newRowIdFieldDefinition() {
    return new PostgreSQLOidFieldDefinition();
  }

  @Override
  protected PostgreSQLRecordStoreSchema newSchema(final JdbcRecordStoreSchema rootSchema,
    final String dbSchemaName, final PathName childSchemaPath) {
    final boolean quoteName = !dbSchemaName.equals(dbSchemaName.toLowerCase());
    return new PostgreSQLRecordStoreSchema((PostgreSQLRecordStoreSchema)rootSchema, childSchemaPath,
      dbSchemaName, quoteName);
  }

  @Override
  protected Map<PathName, ? extends RecordStoreSchemaElement> refreshSchemaElementsDo(
    final JdbcRecordStoreSchema schema, final PathName schemaPath) {
    refreshSchemaElementsDoEnums(schema, schemaPath);
    return super.refreshSchemaElementsDo(schema, schemaPath);
  }

  protected void refreshSchemaElementsDoEnums(final JdbcRecordStoreSchema schema,
    final PathName schemaPath) {
    final Set<PathName> enumTypeNames = Sets.newHash();
    final var sql = """
select
  t.typname as name
from
  pg_type t
join pg_catalog.pg_namespace n on
  n.oid = t.typnamespace
where
  t.typtype = 'e'
  and n.nspname = ?
""";
    try (
      var connection = getJdbcConnection()) {
      try (
        var statement = connection.prepareStatement(sql)) {
        statement.setString(1, schemaPath.getName());
        try (
          var resultSet = statement.executeQuery()) {
          while (resultSet.next()) {
            final var enumName = resultSet.getString(1);
            final var enumType = schemaPath.newChild(enumName);
            enumTypeNames.add(enumType);
          }
        }
      } catch (final SQLException e) {
        throw connection.getException("enum", sql, e);
      }
    } catch (final SQLException e) {
      throw Exceptions.toRuntimeException(e);
    }
    schema.addProperty("enums", enumTypeNames);
  }

  public void setUseSchemaSequencePrefix(final boolean useSchemaSequencePrefix) {
    this.useSchemaSequencePrefix = useSchemaSequencePrefix;
  }

  @Override
  public Array toArray(final Connection connection, final JdbcFieldDefinition field,
    final Collection<?> value) throws SQLException {
    final Collection<?> elements = value;
    final int size = elements.size();
    final Object[] values = new Object[size];
    int i = 0;
    for (final Object element : elements) {
      values[i++] = field.getDataType()
        .toObject(element);
    }
    return connection.unwrap(PgConnection.class)
      .createArrayOf(field.getDbDataType(), values);
  }
}
