package com.revolsys.record.query;

import java.sql.PreparedStatement;

public interface OnConflictAction {
  default int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  void appendSql(SqlAppendable sql);

  default boolean isEmpty() {
    return false;
  }

}
