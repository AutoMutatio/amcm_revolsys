package com.revolsys.record.schema;

import com.revolsys.io.PathName;
import com.revolsys.jdbc.io.JdbcRecordStore;

public class TableRecordStoreImpl extends AbstractTableRecordStore {

  public TableRecordStoreImpl(final PathName typePath, final JdbcRecordStore recordStore) {
    super(typePath, recordStore);
  }

}
