package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.record.schema.RecordStore;

public interface From extends TableReferenceProxy {
  void appendFrom(QueryStatement statement, RecordStore recordStore, final SqlAppendable string);

  default int appendFromParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  default void appendFromWithAlias(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable string) {
    appendFrom(statement, recordStore, string);
  }

  default void appendFromWithAsAlias(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable string) {
    appendFrom(statement, recordStore, string);
  }

  default FromAlias toFromAlias(final String alias) {
    return new FromAlias(this, alias);
  }
}
