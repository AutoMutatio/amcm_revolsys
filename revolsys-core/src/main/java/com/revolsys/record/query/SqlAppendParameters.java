package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Map;

import com.revolsys.collection.iterator.BaseIterable;

public interface SqlAppendParameters {
  static int appendParameters(final PreparedStatement statement, int index,
    Map<String, Object> parameters, final BaseIterable<? extends SqlAppendParameters> clauses) {
    for (final var set : clauses) {
      index = set.appendParameters(index, parameters, statement);
    }
    return index;
  }

  int appendParameters(int index, Map<String, Object> parameters, PreparedStatement statement);
}
