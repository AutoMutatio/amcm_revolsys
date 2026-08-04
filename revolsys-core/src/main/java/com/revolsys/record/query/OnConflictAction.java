package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.Map;

public interface OnConflictAction {
  default int appendParameters(final int index, Map<String, Object> parameters, final PreparedStatement statement) {
    return index;
  }

  void appendSql(SqlAppendable sql);

  default boolean isEmpty() {
    return false;
  }

}
