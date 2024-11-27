package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.revolsys.collection.iterator.BaseIterable;
import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.Record;
import com.revolsys.record.schema.RecordDefinition;

public class InsertStatement extends AbstractReturningQueryStatement<InsertStatement> {

  private final ListEx<ColumnReference> columns = Lists.newArray();

  private final ListEx<ListEx<QueryValue>> values = Lists.newArray();

  private OnConflictAction onConflictAction;

  private final ListEx<ColumnReference> conflictColumns = Lists.newArray();

  private void appendColumns(final SqlAppendable sql) {
    final var columns = this.columns;
    appendColumns(sql, columns);
  }

  private void appendColumns(final SqlAppendable sql, final ListEx<ColumnReference> columns) {
    sql.append("(\n");
    appendColumnNames(sql, columns);
    sql.append(")\n");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final var column : this.columns) {
      index = column.appendParameters(index, statement);
    }
    for (final var valuesRow : this.values) {
      for (final var value : valuesRow) {
        index = value.appendParameters(index, statement);
      }
    }
    if (this.onConflictAction != null) {
      index = this.onConflictAction.appendParameters(index, statement);
    }
    appendReturningParameters(index, statement);
    return index;
  }

  @Override
  public void appendSql(final SqlAppendable sql) {
    sql.append("INSERT INTO ");
    getTable().appendFromWithAsAlias(sql);
    sql.append('\n');
    appendColumns(sql);
    sql.append('\n');
    if (!this.values.isEmpty()) {
      sql.append("VALUES\n");
      appendValues(sql);
    }

    if (this.onConflictAction != null && !this.onConflictAction.isEmpty()) {
      sql.append("ON CONFLICT\n");
      if (!this.conflictColumns.isEmpty()) {
        appendColumns(sql, this.conflictColumns);
        sql.append('\n');
      }
      // TODO conflict target
      this.onConflictAction.appendSql(sql);
    }

    appendReturning(sql);

  }

  private void appendValues(final SqlAppendable sql) {
    boolean first = true;
    for (final var valuesRow : this.values) {
      if (first) {
        first = false;
      } else {
        sql.append(",\n");
      }
      appendValuesRow(sql, valuesRow);
    }
  }

  private void appendValuesRow(final SqlAppendable sql, final ListEx<QueryValue> values) {
    sql.append("(\n");
    boolean first = true;
    for (final var value : values) {
      if (first) {
        first = false;
      } else {
        sql.append(",\n");
      }
      sql.append("  ");
      value.appendSql(this, getRecordStore(), sql);
    }
    sql.append("\n)\n");
  }

  private InsertStatement column(final ColumnReference columnReference) {
    if (columnReference != null && !this.columns.contains(columnReference)) {
      this.columns.add(columnReference);
    }
    return this;
  }

  public InsertStatement column(final Object column) {
    if (column instanceof final ColumnReference columnReference) {
      column(columnReference);
    } else if (column instanceof final CharSequence name) {
      final var columnReference = getTable().getColumn(name);
      column(columnReference);
    } else if (column != null) {
      throw new IllegalArgumentException("Not a valid column class=" + column.getClass());
    }
    return this;
  }

  public InsertStatement columns(final Iterable<Object> columns) {
    for (final var column : columns) {
      column(column);
    }
    return this;
  }

  public InsertStatement columns(final Object... columns) {
    for (final var column : columns) {
      column(column);
    }
    return this;
  }

  private InsertStatement conflictColumn(final ColumnReference columnReference) {
    if (columnReference != null && !this.conflictColumns.contains(columnReference)) {
      this.conflictColumns.add(columnReference);
    }
    return this;
  }

  /**
   * Specify a column where a conflict might occur. For multiple conflict columns,
   * chain calls to this method, or use conflictColumns.
   *
   * @param column
   * @return
   */
  public InsertStatement conflictColumn(final Object column) {
    if (column instanceof final ColumnReference columnReference) {
      conflictColumn(columnReference);
    } else if (column instanceof final CharSequence name) {
      final var columnReference = getTable().getColumn(name);
      conflictColumn(columnReference);
    } else if (column != null) {
      throw new IllegalArgumentException("Not a valid column class=" + column.getClass());
    }
    return this;
  }

  /**
   * Specify multiple columns where conflicts might occur.
   *
   * @param column
   * @return
   */
  public InsertStatement conflictColumns(final Iterable<Object> columns) {
    for (final var column : columns) {
      conflictColumn(column);
    }
    return this;
  }

  public InsertStatement conflictColumns(final Object... columns) {
    for (final var column : columns) {
      conflictColumn(column);
    }
    return this;
  }

  /**
   * Execute this insert statement, and return the number of records inserted and/or updated
   * @return
   */
  public int executeInsertCount() {
    return getRecordStore().executeInsertCount(this);
  }

  public Record executeInsertRecord() {
    return executeInsertRecords(BaseIterable::getFirst);
  }

  public <V> V executeInsertRecords(final Function<BaseIterable<Record>, V> action) {
    return getRecordStore().executeInsertRecords(this, action);
  }

  public ListEx<ColumnReference> getColumns() {
    return this.columns;
  }

  public TableReference getInto() {
    return getTable();
  }

  @Override
  public RecordDefinition getRecordDefinition() {
    if (getTable() == null) {
      return null;
    } else {
      return getTable().getRecordDefinition();
    }
  }

  public InsertStatement insert(final String name, final MapEx source, final String sourceKey) {
    final var value = source.getValue(sourceKey);
    return insert(name, value);
  }

  public InsertStatement insert(final String name, final Object value) {
    if (hasField(name)) {
      final var column = getColumn(name);
      int columnIndex = this.columns.indexOf(column);
      if (columnIndex == -1) {
        columnIndex = this.columns.size();
        column(column);
      }
      if (this.values.size() == 0) {
        this.values.add(Lists.newArray());
      }
      final var values = this.values.get(0);
      while (values.size() <= columnIndex) {
        values.add(null);
      }

      values.set(columnIndex, Value.newValue(column, value));
    } else {
      throw new IllegalArgumentException(getTable() + "." + name + " doesn't exist");
    }
    return this;
  }

  public InsertStatement insertAll(final MapEx values) {
    for (final var entry : values.entrySet()) {
      final var name = entry.getKey();
      final var value = entry.getValue();
      insert(name, value);
    }
    return this;
  }

  public InsertStatement insertAll(final MapEx values, final String... fieldNames) {
    for (final var name : fieldNames) {
      final var value = values.get(name);
      insert(name, value);
    }
    return this;
  }

  public InsertStatement insertFieldValue(final String name, final MapEx source) {
    final var value = source.getValue(name);
    return insert(name, value);
  }

  public InsertStatement insertKey(final String name, final MapEx source, final String sourceKey) {
    final var value = source.getValue(sourceKey);
    return insertKey(name, value);
  }

  /**
   * Specify a primary key column and value for the insert statement, and set this
   * as a conflict column for the case where the record with this key already exists.
   *
   * For compound primary keys, this method can be chained.
   *
   * @param name
   * @param value
   * @return
   */
  public InsertStatement insertKey(final String name, final Object value) {
    insert(name, value);
    conflictColumn(name);
    return this;
  }

  public InsertStatement into(final TableReference table) {
    table(table);
    return this;
  }

  /**
   * Specifies that when there's a conflict on insert, no columns should be updated with
   * the values from the insert statement
   *
   * @param column
   * @return
   */
  public InsertStatement onConflictDoNothing() {
    this.onConflictAction = OnConflictDoNothing.INSTANCE;
    return this;
  }

  private OnConflictDoUpdate onConflictDoUpdate() {
    OnConflictDoUpdate onConfigDoUpdate;
    if (this.onConflictAction instanceof final OnConflictDoUpdate update) {
      onConfigDoUpdate = update;
    } else {
      this.onConflictAction = onConfigDoUpdate = new OnConflictDoUpdate(this);
    }
    return onConfigDoUpdate;
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sqlBuilder = newSqlAppendable();
    sqlBuilder.setUsePlaceholders(false);
    appendSql(sqlBuilder);
    return sqlBuilder.toSqlString();
  }

  public InsertStatement updateAll(final MapEx values) {
    for (final var entry : values.entrySet()) {
      final var name = entry.getKey();
      final var value = entry.getValue();
      insert(name, value);
    }
    return this;
  }

  public InsertStatement updateAll(final MapEx values, final String... fieldNames) {
    for (final var name : fieldNames) {
      final var value = values.get(name);
      update(name, value);
    }
    return this;
  }

  /**
   * Specifies that when there's a conflict on insert, this column should be updated
   * with the value from the insert statement.
   *
   * @param column
   * @return
   */
  public InsertStatement update(final String name) {
    final var column = getColumn(name);
    onConflictDoUpdate().setExcluded(column);
    return this;
  }

  public InsertStatement update(final String name,
    final BiFunction<ColumnReference, Excluded, QueryValue> setProvider) {
    final var column = getColumn(name);
    final var excluded = new Excluded(column);
    final var value = setProvider.apply(column, excluded);
    onConflictDoUpdate().set(column, value);
    return this;
  }

  public InsertStatement update(final String name, final Object value) {
    final var column = getColumn(name);
    onConflictDoUpdate().set(column, value);
    return this;
  }

  /**
   * Insert/update this value for this column when executing the insert statement.
   *
   * If the record already exists and there's a conflict, update the column with
   * this value from the insert statement.
   * This method can be called multiple times for the same insert statement when
   * inserting/updating multiple columns.
   *
   * @param name
   * @param value
   * @return
   */
  public InsertStatement upsert(final String name, final Object value) {
    insert(name, value);
    update(name);
    return this;
  }

  public InsertStatement upsertAll(final MapEx values) {
    for (final var entry : values.entrySet()) {
      final var name = entry.getKey();
      final var value = entry.getValue();
      upsert(name, value);
    }
    return this;
  }

  public InsertStatement upsertAll(final MapEx values, final String... fieldNames) {
    for (final var name : fieldNames) {
      final var value = values.get(name);
      upsert(name, value);
    }
    return this;
  }

}
