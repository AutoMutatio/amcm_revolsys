package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class SqlFragments extends AbstractMultiQueryValue {

  private final DataType dataType;

  public SqlFragments(final DataType dataType, final Iterable<QueryValue> fragments) {
    super(fragments);
    this.dataType = dataType;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    for (final var queryValue : getQueryValues()) {
      queryValue.appendSql(statement, recordStore, sql);
    }
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final var queryValue : getQueryValues()) {
      index = queryValue.appendParameters(index, statement);
    }
    return index;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final int index = indexes.incrementAndGet();
    final Object value = resultSet.getObject(index);
    if (resultSet.wasNull()) {
      return null;
    } else {
      return this.dataType.toObject(value);
    }
  }

}
