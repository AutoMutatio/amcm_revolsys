package com.revolsys.snowflake;

import com.revolsys.io.PathName;
import com.revolsys.jdbc.io.JdbcRecordStoreSchema;

public class SnowflakeRecordStoreSchema extends JdbcRecordStoreSchema {

  public SnowflakeRecordStoreSchema(final SnowflakeRecordStore recordStore) {
    super(recordStore);
  }

  public SnowflakeRecordStoreSchema(final SnowflakeRecordStoreSchema schema,
    final PathName pathName, final String dbName) {
    super(schema, pathName, dbName);
  }

  public SnowflakeRecordStoreSchema(final SnowflakeRecordStoreSchema schema,
    final PathName pathName, final String dbName, final boolean quoteName) {
    super(schema, pathName, dbName, quoteName);
  }
}
