package com.revolsys.record.query;

public interface From {
  void appendFrom(final SqlAppendable string);

  default void appendFromWithAlias(final SqlAppendable string) {
    appendFrom(string);
  }
}
