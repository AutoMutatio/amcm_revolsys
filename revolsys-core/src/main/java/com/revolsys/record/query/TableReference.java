package com.revolsys.record.query;

import java.util.List;

import com.revolsys.collection.list.ListEx;
import com.revolsys.io.PathName;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;

public interface TableReference extends From, TableReferenceProxy {
  static TableReference getTableReference(final RecordDefinitionProxy recordDefinition) {
    if (recordDefinition == null) {
      return null;
    } else {
      return recordDefinition.getRecordDefinition();
    }
  }

  @Override
  default void appendFrom(final SqlAppendable sql) {
    final String tableName = getQualifiedTableName();
    sql.append(tableName);
  }

  @Override
  default void appendFromWithAlias(final SqlAppendable sql) {
    final String tableAlias = getTableAlias();
    appendFromWithAlias(sql, tableAlias);
  }

  default void appendFromWithAlias(final SqlAppendable sql, final String tableAlias) {
    appendFrom(sql);
    if (tableAlias != null) {
      sql.append(" \"");
      sql.append(tableAlias);
      sql.append('"');
    }
  }

  default void appendFromWithAsAlias(final SqlAppendable sql) {
    final String tableAlias = getTableAlias();
    appendFromWithAsAlias(sql, tableAlias);
  }

  default void appendFromWithAsAlias(final SqlAppendable sql, final String tableAlias) {
    appendFrom(sql);
    if (tableAlias != null) {
      sql.append(" AS \"");
      sql.append(tableAlias);
      sql.append('"');
    }
  }

  void appendQueryValue(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue);

  void appendSelect(final QueryStatement statement, final SqlAppendable string,
    final QueryValue queryValue);

  void appendSelectAll(QueryStatement statement, final SqlAppendable string);

  default QueryValue count(final String fieldName) {
    final ColumnReference field = getColumn(fieldName);
    return new Count(field);
  }

  default Condition equal(final CharSequence fieldName, final Object value) {
    final ColumnReference field = getColumn(fieldName);
    QueryValue right;
    if (value == null) {
      return new IsNull(field);
    } else if (value instanceof ColumnReference) {
      right = (ColumnReference)value;
    } else if (value instanceof QueryValue) {
      right = (QueryValue)value;
    } else {
      right = new Value(field, value);
    }
    return new Equal(field, right);
  }

  default Condition equal(final String fieldName, final TableReferenceProxy toTable) {
    return equal(fieldName, toTable, fieldName);
  }

  default Condition equal(final String fromFieldName, final TableReferenceProxy toTable,
    final String toFieldName) {
    final ColumnReference toColumn = toTable.getColumn(toFieldName);
    return equal(fromFieldName, toColumn);
  }

  @Override
  ColumnReference getColumn(final CharSequence name);

  ListEx<FieldDefinition> getFields();

  String getQualifiedTableName();

  RecordDefinition getRecordDefinition();

  @Override
  String getTableAlias();

  PathName getTablePath();

  @Override
  default TableReference getTableReference() {
    return this;
  }

  boolean hasColumn(CharSequence name);

  default ILike iLike(final String fieldName, final Object value) {
    final ColumnReference field = getColumn(fieldName);
    final Value valueCondition = Value.newValue(value);
    return new ILike(field, valueCondition);
  }

  default Condition in(final String fieldName, final List<?> list) {
    final ColumnReference field = getColumn(fieldName);
    final CollectionValue right = new CollectionValue(field, list);
    return new In(field, right);
  }

  default IsNotNull isNotNull(final CharSequence fieldName) {
    final ColumnReference field = getColumn(fieldName);
    return new IsNotNull(field);
  }

  default IsNull isNull(final CharSequence fieldName) {
    final ColumnReference field = getColumn(fieldName);
    return new IsNull(field);
  }
}
