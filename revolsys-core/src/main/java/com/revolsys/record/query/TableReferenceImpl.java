package com.revolsys.record.query;

import com.revolsys.collection.list.ListEx;
import com.revolsys.io.PathName;
import com.revolsys.jdbc.JdbcUtils;
import com.revolsys.record.schema.FieldDefinition;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public class TableReferenceImpl implements TableReference {

  private final RecordDefinition recordDefinition;

  private final PathName tablePath;

  private final String tableAlias;

  private final String qualifiedTableName;

  public TableReferenceImpl(final PathName tablePath) {
    this(tablePath, null);
  }

  public TableReferenceImpl(final PathName tablePath, final String alias) {
    this.recordDefinition = null;
    this.tablePath = tablePath;
    this.tableAlias = alias;
    this.qualifiedTableName = JdbcUtils.getQualifiedTableName(tablePath);
  }

  public TableReferenceImpl(final RecordDefinitionProxy recordDefinition) {
    this(recordDefinition, null);
  }

  public TableReferenceImpl(final RecordDefinitionProxy recordDefinition, final String alias) {
    if (recordDefinition == null) {
      this.recordDefinition = null;
    } else {
      this.recordDefinition = recordDefinition.getRecordDefinition();
    }
    this.tablePath = this.recordDefinition.getPathName();
    this.tableAlias = alias;
    this.qualifiedTableName = this.recordDefinition.getQualifiedTableName();

  }

  @Override
  public void appendQueryValue(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final RecordDefinition recordDefinition = this.recordDefinition;
    if (recordDefinition == null) {
      queryValue.appendSql(statement, null, sql);
    } else {
      final RecordStore recordStore = recordDefinition.getRecordStore();
      queryValue.appendSql(statement, recordStore, sql);
    }
  }

  @Override
  public void appendSelect(final QueryStatement statement, final SqlAppendable sql,
    final QueryValue queryValue) {
    final RecordDefinition recordDefinition = this.recordDefinition;
    if (recordDefinition == null) {
      queryValue.appendSelect(statement, statement.getRecordStore(), sql);
    } else {
      recordDefinition.appendSelect(statement, sql, queryValue);
    }
  }

  @Override
  public void appendSelectAll(final QueryStatement statement, final SqlAppendable sql) {
    if (this.recordDefinition == null) {
      appendColumnPrefix(sql);
      sql.append('*');
    } else {
      this.recordDefinition.appendSelectAll(statement, sql);
    }
  }

  @Override
  public ColumnReference getColumn(final CharSequence name) {
    if (this.recordDefinition == null) {
      return new Column(this, name);
    } else {
      return this.recordDefinition.getColumn(name);
    }
  }

  @Override
  public ListEx<FieldDefinition> getFields() {
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition == null) {
      return ListEx.empty();
    } else {
      return recordDefinition.getFields();
    }
  }

  @Override
  public String getQualifiedTableName() {
    return this.qualifiedTableName;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    return this.recordDefinition;
  }

  @Override
  public String getTableAlias() {
    return this.tableAlias;
  }

  @Override
  public PathName getTablePath() {
    return this.tablePath;
  }

  @Override
  public boolean hasColumn(final CharSequence name) {
    if (this.recordDefinition == null) {
      return false;
    } else {
      return this.recordDefinition.hasColumn(name);
    }
  }

  public boolean hasField(final CharSequence name) {
    if (this.recordDefinition == null) {
      return false;
    } else {
      return this.recordDefinition.hasField(name);
    }
  }

  @Override
  public String toString() {
    if (this.tableAlias == null) {
      return this.qualifiedTableName.toString();
    } else {
      return this.qualifiedTableName + " \"" + this.tableAlias + '"';
    }
  }
}
