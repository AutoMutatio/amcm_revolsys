package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.Consumer;

import com.revolsys.collection.list.ArrayListEx;
import com.revolsys.collection.list.ListEx;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordDefinitionProxy;
import com.revolsys.record.schema.RecordStore;

public class UpdateStatement implements RecordDefinitionProxy {
  private record UpdateSetClause(ColumnReference column, QueryValue value) {

    public void appendSql(final UpdateStatement update, final SqlAppendable sql) {
      this.column.appendColumnName(sql);
      sql.append(" = ");
      if (this.value == null) {
        sql.append("null");
      } else {
        this.value.appendSql(null, update.getRecordStore(), sql);
      }
    }

    public int appendParameters(int index, final PreparedStatement statement) {
      index = this.column.appendParameters(index, statement);
      if (this.value != null) {
        index = this.value.appendParameters(index, statement);
      }
      return index;
    }
  }

  private TableReference table;

  private final ListEx<UpdateSetClause> setClauses = new ArrayListEx<>();

  private Condition where;

  public int appendParameters(int index, final PreparedStatement statement) {
    for (final UpdateSetClause set : this.setClauses) {
      index = set.appendParameters(index, statement);
    }

    final Condition where = getWhere();
    if (!where.isEmpty()) {
      index = where.appendParameters(index, statement);
    }
    return index;
  }

  public void appendSql(final SqlAppendable sql) {
    final RecordStore recordStore = getRecordStore();
    sql.append("UPDATE ");
    this.table.appendFromWithAlias(sql);

    sql.append(" SET ");
    if (this.setClauses.isEmpty()) {
      throw new IllegalStateException("Update statement must set at least one value");
    }
    boolean first = true;
    for (final UpdateSetClause set : this.setClauses) {
      if (first) {
        first = false;
      } else {
        sql.append(", ");
      }
      set.appendSql(this, sql);
    }
    final Condition where = this.where;
    if (where != null && !where.isEmpty()) {
      sql.append(" WHERE  ");
      where.appendSql(null, recordStore, sql);
    }
  }

  public UpdateStatement from(final TableReference from) {
    this.table = from;
    return this;
  }

  public TableReference getFrom() {
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
    this.setClauses.add(new UpdateSetClause(column, value));
    return this;
  }

  public UpdateStatement setNull(final CharSequence name) {
    return set(name, null);
  }

  public UpdateStatement setNull(final ColumnReference column) {
    return set(column, null);
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

  public int updateRecords() {
    return getRecordStore().updateRecords(this);
  }

  public UpdateStatement where(final Condition where) {
    this.where = where;
    return this;
  }

  public UpdateStatement where(final Consumer<WhereConditionBuilder> action) {
    final WhereConditionBuilder builder = new WhereConditionBuilder(getFrom());
    this.where = builder.build(action);
    return this;
  }

  public UpdateStatement where(final String fieldName, final Object value) {
    final ColumnReference left = this.table.getColumn(fieldName);
    if (value == null) {
      this.where = new IsNull(left);
    } else {
      QueryValue right;
      if (value instanceof final QueryValue queryValue) {
        right = queryValue;
      } else {
        right = new Value(left, value);
      }
      this.where = new Equal(left, right);
    }
    return this;
  }

}
