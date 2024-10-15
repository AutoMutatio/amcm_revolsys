package com.revolsys.record.query;

import java.sql.PreparedStatement;

public interface From extends TableReferenceProxy {
  void appendFrom(final SqlAppendable string);

  default int appendFromParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  default void appendFromWithAlias(final SqlAppendable string) {
    appendFrom(string);
  }
}
