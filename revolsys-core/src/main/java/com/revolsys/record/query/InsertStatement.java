package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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

  private ConflictAction conflictAction;

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
    if (this.conflictAction != null) {
      index = this.conflictAction.appendParameters(index, statement);
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

    if (this.conflictAction != null && !this.conflictAction.isEmpty()) {
      sql.append("ON CONFLICT\n");
      if (!this.conflictColumns.isEmpty()) {
        appendColumns(sql, this.conflictColumns);
        sql.append('\n');
      }
      // TODO conflict target
      this.conflictAction.appendSql(sql);
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

  public InsertStatement conflictDoNothing() {
    this.conflictAction = ConflictDoNothing.INSTANCE;
    return this;
  }

  private ConflictUpdate conflictUpdate() {
    ConflictUpdate configUpdate;
    if (this.conflictAction instanceof final ConflictUpdate update) {
      configUpdate = update;
    } else {
      this.conflictAction = configUpdate = new ConflictUpdate(this);
    }
    return configUpdate;
  }

  public boolean executeInsert() {
    return executeInsertCount() > 0;
  }

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

  public InsertStatement insertKey(final String name, final Object value) {
    insert(name, value);
    conflictColumn(name);
    return this;
  }

  public InsertStatement into(final TableReference table) {
    table(table);
    return this;
  }

  @Override
  public String toString() {
    final StringBuilderSqlAppendable sqlBuilder = newSqlAppendable();
    sqlBuilder.setUsePlaceholders(false);
    appendSql(sqlBuilder);
    return sqlBuilder.toSqlString();
  }

  public InsertStatement update(final String name) {
    final var column = getColumn(name);
    conflictUpdate().setExisting(column);
    return this;
  }

  public InsertStatement update(final String name,
    final BiFunction<ColumnReference, Excluded, QueryValue> setProvider) {
    final var column = getColumn(name);
    final var excluded = new Excluded(column);
    final var value = setProvider.apply(column, excluded);
    conflictUpdate().set(column, value);
    return this;
  }

  public InsertStatement update(final String name, final Object value) {
    final var column = getColumn(name);
    conflictUpdate().set(column, value);
    return this;
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

  public InsertStatement updateWhere(final Consumer<WhereConditionBuilder> where) {
    conflictUpdate().where(where);
    return this;
  }

  /**
   * Use the value for an insert and also set that field in the on conflict do update
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
