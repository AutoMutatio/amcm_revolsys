package com.revolsys.record.query;

import com.revolsys.record.schema.FieldDefinition;

public interface TableReferenceProxy {

  default void appendColumnPrefix(final SqlAppendable string) {
    final String alias = getTableAlias();
    if (alias != null) {
      string.append(alias);
      string.append('.');
    }
  }

  default ColumnReference getColumn(final CharSequence name) {
    return getTableReference().getColumn(name);
  }

  default FieldDefinition getField(final CharSequence name) {
    final ColumnReference column = getColumn(name);
    if (column instanceof FieldDefinition) {
      return (FieldDefinition)column;
    }
    return null;
  }

  default String getTableAlias() {
    final TableReference table = getTableReference();
    return table.getTableAlias();
  }

  TableReference getTableReference();
}
