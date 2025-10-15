package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public interface From extends TableReferenceProxy, QueryValue {
  @Override
  default void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendFromWithAlias(statement, recordStore, sql);
  }

  @Override
  default void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    appendFromWithAlias(statement, recordStore, sql);
  }

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

  @Override
  default int appendParameters(final int index, final PreparedStatement statement) {
    return appendFromParameters(index, statement);
  }

  @Override
  default QueryValue clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  default <V> V getValue(final MapEx record) {
    return null;
  }

  default FromAlias toFromAlias(final String alias) {
    return new FromAlias(this, alias);
  }
}
