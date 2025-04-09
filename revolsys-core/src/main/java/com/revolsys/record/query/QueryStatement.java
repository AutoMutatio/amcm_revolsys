package com.revolsys.record.query;

import com.revolsys.record.schema.RecordDefinitionProxy;

public interface QueryStatement extends RecordDefinitionProxy {

  default void appendQueryValue(final SqlAppendable sql, final QueryValue queryValue) {
    final var recordStore = getRecordStore();
    queryValue.appendSql(this, recordStore, sql);
  }

}
