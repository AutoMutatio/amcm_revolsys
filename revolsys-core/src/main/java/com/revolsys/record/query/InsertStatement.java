package com.revolsys.record.query;

import java.sql.PreparedStatement;

import org.jeometry.common.collection.list.ArrayListEx;
import org.jeometry.common.collection.list.ListEx;

import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public class InsertStatement implements RecordDefinitionProxy {
  private record UpdateSetClause(ColumnReference column, QueryValue value) {

    public void appendSql(final InsertStatement update, final SqlAppendable sql) {
      this.column.appendColumnName(sql);
      sql.append(" = ");
      this.value.appendSql(null, update.getRecordStore(), sql);
    }

    public int appendParameters(int index, final PreparedStatement statement) {
      index = this.column.appendParameters(index, statement);
      index = this.value.appendParameters(index, statement);
      return index;
    }
  }

  private TableReference table;

  private final ListEx<String> columnNames = new ArrayListEx<>();

  public int appendParameters(final int index, final PreparedStatement statement) {

    return index;
  }

  public void appendSql(final SqlAppendable sql) {
    final RecordStore recordStore = getRecordStore();
    sql.append("INSERT INTO ");
    this.table.appendFromWithAlias(sql);

  }

  public InsertStatement column(final String name) {
    this.columnNames.add(name);
    return this;
  }

  public TableReference getInto() {
    return this.table;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getRecordDefinition();
    }
  }

  public int insertRecords() {
    return getRecordStore().insertRecords(this);
  }

  public InsertStatement into(final TableReference from) {
    this.table = from;
    return this;
  }

  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition != null) {
      sql.setRecordStore(recordDefinition.getRecordStore());
    }
    return sql;
  }

  public String toSql() {
    final StringBuilderSqlAppendable sql = newSqlAppendable();
    appendSql(sql);
    return sql.toSqlString();
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sqlBuilder = newSqlAppendable();
    sqlBuilder.setUsePlaceholders(false);
    appendSql(sqlBuilder);
    return sqlBuilder.toSqlString();
  }

}
