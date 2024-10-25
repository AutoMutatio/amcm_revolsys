package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;

public class InsertStatement implements RecordDefinitionProxy {
  private TableReference table;

  private final ListEx<String> columnNames = new ArrayListEx<>();

  private Conflict conflict;

  public int appendParameters(final int index, final PreparedStatement statement) {

    return index;
  }

  public void appendSql(final SqlAppendable sql) {
    sql.append("INSERT INTO ");
    this.table.appendFromWithAlias(sql);
    // TODO columns, overriding, dfault values |vaues

    if (this.conflict != null) {
      this.conflict.appendSql(sql);
    }

  }

  public InsertStatement column(final String name) {
    this.columnNames.add(name);
    return this;
  }

  public InsertStatement conflict(final Consumer<Conflict> action) {
    this.conflict = new Conflict(this);
    action.accept(this.conflict);
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
