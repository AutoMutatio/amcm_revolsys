package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public class DeleteStatement implements RecordDefinitionProxy {

  private TableReference from;

  private Condition where;

  public int appendParameters(int index, final PreparedStatement statement) {
    final Condition where = getWhere();
    if (!where.isEmpty()) {
      index = where.appendParameters(index, statement);
    }
    return index;
  }

  public void appendSql(final SqlAppendable sql) {
    final RecordStore recordStore = getRecordStore();
    sql.append("DELETE FROM ");
    this.from.appendFromWithAlias(sql);
    final Condition where = this.where;
    if (where != null && !where.isEmpty()) {
      sql.append(" WHERE  ");
      where.appendSql(null, recordStore, sql);
    }
  }

  public int deleteRecords() {
    return getRecordStore().deleteRecords(this);
  }

  public DeleteStatement from(final TableReference from) {
    this.from = from;
    return this;
  }

  public TableReference getFrom() {
    return this.from;
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.from == null) {
      return null;
    } else {
      return this.from.getRecordDefinition();
    }
  }

  public Condition getWhere() {
    return this.where;
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

  public DeleteStatement where(final Condition where) {
    this.where = where;
    return this;
  }

  public DeleteStatement where(final Consumer<WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(getFrom());
    this.where = builder.build(action);
    return this;
  }

}
