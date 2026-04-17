package com.revolsys.mssqlserver;

import com.revolsys.io.PathName;
import com.revolsys.jdbc.io.JdbcRecordStoreSchema;

public class MicrosoftSqlServerRecordStoreSchema extends JdbcRecordStoreSchema {

  public MicrosoftSqlServerRecordStoreSchema(final MicrosoftSqlServerRecordStore recordStore) {
    super(recordStore);
  }

  public MicrosoftSqlServerRecordStoreSchema(final MicrosoftSqlServerRecordStoreSchema schema,
    final PathName pathName, final String dbName) {
    super(schema, pathName, dbName);
  }

  public MicrosoftSqlServerRecordStoreSchema(final MicrosoftSqlServerRecordStoreSchema schema,
    final PathName pathName, final String dbName, final boolean quoteName) {
    super(schema, pathName, dbName, quoteName);
  }
}
