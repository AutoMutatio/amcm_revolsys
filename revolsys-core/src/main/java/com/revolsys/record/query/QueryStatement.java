package com.revolsys.record.query;

import com.revolsys.record.schema.RecordDefinitionProxy;

public interface QueryStatement extends RecordDefinitionProxy {
  public static final QueryStatement EMPTY = () -> null;

  default void appendFrom(final SqlAppendable sql, final From from) {
    final var recordStore = getRecordStore();
    from.appendFrom(this, recordStore, sql);
  }

  default void appendFromWithAlias(final SqlAppendable sql, final From from) {
    final var recordStore = getRecordStore();
    from.appendFromWithAlias(this, recordStore, sql);
  }

  default void appendFromWithAsAlias(final SqlAppendable sql, final From from) {
    final var recordStore = getRecordStore();
    from.appendFromWithAsAlias(this, recordStore, sql);
  }

  default void appendQueryValue(final SqlAppendable sql, final QueryValue queryValue) {
    final var recordStore = getRecordStore();
    queryValue.appendSql(this, recordStore, sql);
  }
}
