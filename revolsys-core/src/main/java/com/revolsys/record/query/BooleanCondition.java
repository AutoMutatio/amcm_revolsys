package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public record BooleanCondition(boolean value) implements Condition {
  public static final BooleanCondition TRUE = new BooleanCondition(true);

  public static final BooleanCondition FALSE = new BooleanCondition(false);

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append(this.value);
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    return index;
  }

  @Override
  public BooleanCondition clone() {
    return this;
  }

  @Override
  public BooleanCondition clone(final TableReference oldTable, final TableReference newTable) {
    return this;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof final BooleanCondition condition) {
      return this.value == condition.value;
    }
    return false;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return this.value;
  }

  @Override
  public String toString() {
    return Boolean.toString(this.value);
  }
}
