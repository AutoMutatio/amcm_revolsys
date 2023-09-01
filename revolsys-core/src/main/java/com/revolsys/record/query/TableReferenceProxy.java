package com.revolsys.record.query;

import com.revolsys.record.schema.FieldDefinition;

public interface TableReferenceProxy {
  default void appendColumnPrefix(final SqlAppendable string) {
    final String alias = getTableAlias();
    if (alias != null) {
      string.append('"');
      string.append(alias);
      string.append('"');
      string.append('.');
    }
  }

  default DeleteStatement deleteStatement() {
    return new DeleteStatement().from(getTableReference());
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
    if (table == null) {
      return null;
    } else {
      return table.getTableAlias();
    }
  }

  TableReference getTableReference();
}
