package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ConflictUpdate implements ConflictAction {

  private final Map<ColumnReference, QueryValue> setExpressions = new LinkedHashMap<>();

  private final QueryStatement statement;

  private Condition where = Condition.ALL;

  public ConflictUpdate(final QueryStatement statement) {
    this.statement = statement;
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final var entry : this.setExpressions.entrySet()) {
      final var column = entry.getKey();
      final var value = entry.getValue();
      index = column.appendParameters(index, statement);
      index = value.appendParameters(index, statement);
    }
    index = this.where.appendParameters(index, statement);
    return index;
  }

  @Override
  public void appendSql(final SqlAppendable sql) {
    sql.append("DO UPDATE SET\n");
    boolean first = true;
    for (final var entry : this.setExpressions.entrySet()) {
      if (first) {
        first = false;
      } else {
        sql.append(",\n");
      }
      sql.append("  ");
      final var column = entry.getKey();
      final var value = entry.getValue();
      column.appendColumnName(sql);
      sql.append(" = ");
      value.appendSql(this.statement, sql);
    }
    sql.append('\n');
    if (!this.where.isEmpty()) {
      sql.append("WHERE\n");
      this.where.appendSql(this.statement, sql);
      sql.append('\n');
    }
  }

  @Override
  public boolean isEmpty() {
    return this.setExpressions.isEmpty();
  }

  public void set(final ColumnReference column, final Object value) {
    final QueryValue queryValue;
    if (value instanceof final QueryValue qv) {
      queryValue = qv;
    } else {
      queryValue = Value.newValue(column, value);
    }
    this.setExpressions.put(column, queryValue);
  }

  public void setExisting(final ColumnReference column) {
    this.setExpressions.put(column, new Excluded(column));
  }

  public ConflictUpdate where(final Consumer<WhereConditionBuilder> action) {
    final var table = this.statement.getRecordDefinition();
    this.where = new WhereConditionBuilder(table, this.where).build(action);
    return this;
  }
}
