package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;

public class UpdateStatement extends AbstractReturningQueryStatement<UpdateStatement> {
  private final ListEx<From> fromClauses = Lists.newArray();

  private final ListEx<SetClause> setClauses = new ArrayListEx<>();

  private Condition where = Condition.ALL;

  private final ListEx<With> withClauses = Lists.newArray();

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    index = SqlAppendParameters.appendParameters(statement, index, this.withClauses);
    index = SqlAppendParameters.appendParameters(statement, index, this.setClauses);
    for (final var fromClause : this.fromClauses) {
      if (!(fromClause instanceof With)) {
        index = fromClause.appendFromParameters(index, statement);
      }
    }
    final Condition where = getWhere();
    if (!where.isEmpty()) {
      index = where.appendParameters(index, statement);
    }
    return index;
  }

  @Override
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
    appendFromWithAlias(sql, getTable());

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
        appendFrom(sql, from);
      }
      sql.append(' ');
    }
    final Condition where = this.where;
    if (where != null && !where.isEmpty()) {
      sql.append(" WHERE  ");
      where.appendSql(this, sql);
    }
    appendReturning(sql);
  }

  public UpdateStatement apply(final Consumer<UpdateStatement> action) {
    action.accept(this);
    return this;
  }

  public boolean executeUpdate() {
    return executeUpdateCount() > 0;
  }

  public int executeUpdateCount() {
    return getRecordStore().executeUpdateCount(this);
  }

  public Record executeUpdateRecord() {
    return executeUpdateRecords(BaseIterable::getFirst);
  }

  public <V> V executeUpdateRecords(final Function<BaseIterable<Record>, V> action) {
    return getRecordStore().executeUpdateRecords(this, action);
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (getTable() == null) {
      return null;
    } else {
      return getTable().getRecordDefinition();
    }
  }

  public Condition getWhere() {
    return this.where;
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

  public UpdateStatement setFieldValue(final String fieldName, final MapEx source) {
    return setFieldValue(fieldName, source, fieldName);
  }

  public UpdateStatement setFieldValue(final String fieldName, final MapEx source,
    final String sourceKey) {
    final var value = source.getValue(sourceKey);
    return set(fieldName, value);
  }

  public UpdateStatement setNull(final CharSequence name) {
    return set(name, null);
  }

  public UpdateStatement setNull(final ColumnReference column) {
    return set(column, null);
  }

  public TableReference table() {
    return getTable();
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

  public UpdateStatement updateKeyFieldValue(final String fieldName, final MapEx source) {
    return updateKeyFieldValue(fieldName, source, fieldName);
  }

  public UpdateStatement updateKeyFieldValue(final String fieldName, final MapEx source,
    final String sourceKey) {
    final var value = source.getValue(sourceKey);
    return updateKey(fieldName, value);
  }

  public UpdateStatement where(final Consumer<WhereConditionBuilder> action) {
    final var table = table();
    this.where = new WhereConditionBuilder(table, this.where).build(action);
    return this;
  }

  public UpdateStatement with(final BiConsumer<UpdateStatement, With> action) {
    final var with = new With();
    this.withClauses.add(with);
    this.fromClauses.add(with);
    action.accept(this, with);
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
