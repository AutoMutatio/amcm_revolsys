package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.record.schema.RecordDefinition;

@SuppressWarnings("unchecked")
public abstract class AbstractReturningQueryStatement<QS extends QueryStatement>
  implements QueryStatement {

  private final ListEx<ColumnReference> returning = Lists.newArray();

  private boolean returningAll = false;

  TableReference table;

  public AbstractReturningQueryStatement() {
    super();
  }

  protected void appendColumnNames(final SqlAppendable sql, final ListEx<ColumnReference> columns) {
    boolean first = true;
    for (final var column : columns) {
      if (first) {
        first = false;
      } else {
        sql.append(",\n");
      }
      sql.append("  ");
      column.appendColumnName(sql);
    }
    sql.append('\n');
  }

  public abstract int appendParameters(int index, PreparedStatement statement);

  protected void appendReturning(final SqlAppendable sql) {
    if (this.returningAll) {
      sql.append("RETURNING *\n");
    } else if (!this.returning.isEmpty()) {
      sql.append("RETURNING\n");
      appendColumnNames(sql, this.returning);
    }
  }

  protected int appendReturningParameters(int index, final PreparedStatement statement) {
    for (final var column : this.returning) {
      index = column.appendParameters(index, statement);
    }
    return index;
  }

  protected abstract void appendSql(SqlAppendable sql);

  public QS ensureReturning() {
    if (!this.returningAll || this.returning.isEmpty()) {
      this.returningAll = true;
    }
    return (QS)this;
  }

  public ListEx<ColumnReference> getReturning() {
    return this.returning;
  }

  public TableReference getTable() {
    return this.table;
  }

  public boolean isReturningAll() {
    return this.returningAll;
  }

  protected StringBuilderSqlAppendable newSqlAppendable() {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    final RecordDefinition recordDefinition = getRecordDefinition();
    if (recordDefinition != null) {
      sql.setRecordStore(recordDefinition.getRecordStore());
    }
    return sql;
  }

  private QS returning(final ColumnReference column) {
    if (column != null && !this.returning.contains(column)) {
      this.returning.add(column);
    }
    return (QS)this;
  }

  public QS returning(final Iterable<Object> returning) {
    for (final var column : returning) {
      returning(column);
    }
    return (QS)this;
  }

  public QS returning(final Object column) {
    if (column instanceof final ColumnReference columnReference) {
      returning(columnReference);
    } else if (column instanceof final CharSequence name) {
      final var columnReference = this.getTable()
        .getColumn(name);
      returning(columnReference);
    } else if (column != null) {
      throw new IllegalArgumentException("Not a valid column class=" + column.getClass());
    }
    return (QS)this;
  }

  public QS returning(final Object... returning) {
    for (final var column : returning) {
      returning(column);
    }
    return (QS)this;
  }

  public QS returningAll() {
    this.returningAll = true;
    return (QS)this;
  }

  public QS table(final TableReference table) {
    this.table = table;
    return (QS)this;
  }

  public String toSql() {
    final StringBuilderSqlAppendable sql = newSqlAppendable();
    appendSql(sql);
    return sql.toSqlString();
  }
}
