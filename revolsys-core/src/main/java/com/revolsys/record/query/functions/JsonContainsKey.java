package com.revolsys.record.query.functions;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.exception.Exceptions;
import com.revolsys.record.query.AbstractUnaryQueryValue;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Query;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordStore;

public class JsonContainsKey extends AbstractUnaryQueryValue implements Condition {

  private final String key;

  public JsonContainsKey(final QueryValue left, final String key) {
    super(left);
    this.key = key;
  }

  @Override
  public void appendDefaultSql(final Query query, final RecordStore recordStore,
    final SqlAppendable sql) {
    getValue().appendSql(query, recordStore, sql);
    sql.append(" ?? ?");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    final var left = getValue();
    if (left != null) {
      index = left.appendParameters(index, statement);
    }
    try {
      statement.setString(index++, this.key);
    } catch (final SQLException e) {
      Exceptions.throwUncheckedException(e);
    }
    return index;
  }

  @Override
  public JsonContainsKey clone() {
    return (JsonContainsKey)super.clone();
  }

  @Override
  public JsonContainsKey clone(final TableReference oldTable, final TableReference newTable) {
    return (JsonContainsKey)super.clone(oldTable, newTable);
  }

  @Override
  public boolean test(final MapEx record) {
    final QueryValue left = getValue();
    final Object value1 = left.getValue(record);

    if (value1 instanceof JsonObject) {
      final JsonObject jsonObject = (JsonObject)value1;
      return jsonObject.containsKey(this.key);
    }
    if (value1 instanceof List) {
      final List<?> jsonObject = (List<?>)value1;
      return jsonObject.contains(this.key);
    } else {
      return false;
    }
  }

}
