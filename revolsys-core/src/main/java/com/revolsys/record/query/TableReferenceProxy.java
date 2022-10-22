package com.revolsys.record.query;

public interface TableReferenceProxy {

  default ColumnReference getColumn(final CharSequence name) {
    return getTableReference().getColumn(name);
  }

  TableReference getTableReference();
}
