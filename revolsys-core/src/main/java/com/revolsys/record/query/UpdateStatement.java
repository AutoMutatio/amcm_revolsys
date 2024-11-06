package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.record.schema.RecordDefinition;

public class UpdateStatement implements QueryStatement {
  private final ListEx<From> fromClauses = Lists.newArray();

  private final ListEx<SetClause> setClauses = new ArrayListEx<>();

  private TableReference table;

  private Condition where = Condition.ALL;

  private final ListEx<With> withClauses = Lists.newArray();

  public int appendParameters(int index, final PreparedStatement statement) {
    index = SqlAppendParameters.appendParameters(statement, index, this.withClauses);
    index = SqlAppendParameters.appendParameters(statement, index, this.setClauses);
    for (final var set : this.fromClauses) {
      index = set.appendFromParameters(index, statement);
    }
    final Condition where = getWhere();
    if (!where.isEmpty()) {
      index = where.appendParameters(index, statement);
    }
    return index;
  }

  public void appendSql(final SqlAppendable sql) {
    if (!this.withClauses.isEmpty()) {
      sql.append("WITH ");
      boolean first = true;
      for (final var with : this.withClauses) {
        if (first) {
          first = false;
        } else {
          sql.append(", ");
        }
        with.appendSql(sql);
      }
      sql.append(' ');
    }
    sql.append("UPDATE ");
    this.table.appendFromWithAlias(sql);

    if (this.setClauses.isEmpty()) {
      throw new IllegalStateException("Update statement must set at least one value");
    }
    SetClause.appendSet(sql, this, this.setClauses);
    if (!this.fromClauses.isEmpty()) {
      sql.append(" FROM  ");
      boolean first = true;
      for (final var from : this.fromClauses) {
        if (first) {
          first = false;
        } else {
          sql.append(", ");
        }
        from.appendFrom(sql);
      }
      sql.append(' ');
    }
    final Condition where = this.where;
    if (where != null && !where.isEmpty()) {
      sql.append(" WHERE  ");
      where.appendSql(this, sql);
    }
  }

  public int executeUpdateCount() {
    return getRecordStore().updateRecords(this);
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (this.table == null) {
      return null;
    } else {
      return this.table.getRecordDefinition();
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

  public UpdateStatement set(final CharSequence name, final Object value) {
    final ColumnReference column = getColumn(name);
    final Value queryValue = new Value(column, value);
    return set(column, queryValue);
  }

  public UpdateStatement set(final CharSequence name, final QueryValue value) {
    final ColumnReference column = getColumn(name);
    return set(column, value);
  }

  public UpdateStatement set(final ColumnReference column, final QueryValue value) {
    this.setClauses.add(new SetClause(this, column, value));
    return this;
  }

  public UpdateStatement setNull(final CharSequence name) {
    return set(name, null);
  }

  public UpdateStatement setNull(final ColumnReference column) {
    return set(column, null);
  }

  public TableReference table() {
    return this.table;
  }

  public UpdateStatement table(final TableReference from) {
    this.table = from;
    return this;
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

  /**
   * Add a where condition for this field and value.
   *
   * @param fieldName
   * @param value
   * @return
   */
  public UpdateStatement updateKey(final String fieldName, final Object value) {
    return where(w -> w.and(fieldName, value));
  }

  public UpdateStatement where(final Consumer<WhereConditionBuilder> action) {
    final var table = table();
    this.where = new WhereConditionBuilder(table, this.where).build(action);
    return this;
  }

  public UpdateStatement with(final Consumer<With> action) {
    final var with = new With();
    this.withClauses.add(with);
    this.fromClauses.add(with);
    action.accept(with);
    return this;
  }

}
