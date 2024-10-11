package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.iterator.BaseIterable;

public interface SqlAppendParameters {
  static int appendParameters(final PreparedStatement statement, int index,
    final BaseIterable<? extends SqlAppendParameters> clauses) {
    for (final var set : clauses) {
      index = set.appendParameters(index, statement);
    }
    return index;
  }

  int appendParameters(int index, PreparedStatement statement);
}
