package com.revolsys.record.query;

import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public class JsonConcatenate extends AbstractBinaryQueryValue {

  public JsonConcatenate(final QueryValue left, final QueryValue right) {
    super(left, right);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    appendLeft(statement, recordStore, buffer);
    buffer.append(" || ");
    appendRight(statement, recordStore, buffer);
  }

  @Override
  public JsonConcatenate clone() {
    return (JsonConcatenate)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof JsonConcatenate) {
      return super.equals(obj);
    }
    return false;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    return null;
  }
}
