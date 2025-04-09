package com.revolsys.record.query;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class ConditionHolder implements Condition {

  private Condition condition;

  public ConditionHolder() {
  }

  public ConditionHolder(final Condition condition) {
    this.condition = condition;
  }

  @Override
  public void appendDefaultSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (this.condition != null) {
      this.condition.appendDefaultSelect(statement, recordStore, sql);
    }
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (this.condition != null) {
      this.condition.appendDefaultSql(statement, recordStore, sql);
    }
  }

  @Override
  public int appendParameters(final int index, final PreparedStatement statement) {
    if (this.condition == null) {
      return index;
    } else {
      return this.condition.appendParameters(index, statement);
    }
  }

  @Override
  public void appendSelect(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    if (this.condition != null) {
      this.condition.appendSelect(statement, recordStore, sql);
    }
  }

  @Override
  public void appendSql(final QueryStatement statement, final RecordStore recordStore, final SqlAppendable sql) {
    if (this.condition != null) {
      this.condition.appendSql(statement, recordStore, sql);
    }
  }

  @Override
  public void changeRecordDefinition(final RecordDefinition oldRecordDefinition,
    final RecordDefinition newRecordDefinition) {
    if (this.condition != null) {
      this.condition.changeRecordDefinition(oldRecordDefinition, newRecordDefinition);
    }
  }

  @Override
  public ConditionHolder clone() {
    return new ConditionHolder(this.condition);
  }

  @Override
  public Condition clone(final TableReference oldTable, final TableReference newTable) {
    if (this.condition == null) {
      return new ConditionHolder(this.condition);
    } else {
      final Condition condition = this.condition.clone(oldTable, newTable);
      return new ConditionHolder(condition);
    }
  }

  public Condition getCondition() {
    return this.condition;
  }

  @Override
  public int getFieldIndex() {
    if (this.condition == null) {
      return -1;
    } else {
      return this.condition.getFieldIndex();
    }
  }

  @Override
  public List<QueryValue> getQueryValues() {
    if (this.condition == null) {
      return Collections.emptyList();
    } else {
      return this.condition.getQueryValues();
    }
  }

  @Override
  public String getStringValue(final MapEx record) {
    if (this.condition == null) {
      return null;
    } else {
      return this.condition.getStringValue(record);
    }
  }

  @Override
  public <V> V getValue(final MapEx record) {
    if (this.condition == null) {
      return null;
    } else {
      return this.condition.getValue(record);
    }
  }

  @Override
  public <V> V getValue(final MapEx record, final DataType dataType) {
    if (this.condition == null) {
      return null;
    } else {
      return this.condition.getValue(record, dataType);
    }
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    if (this.condition == null) {
      return null;
    } else {
      return this.condition.getValueFromResultSet(recordDefinition, resultSet, indexes,
        internStrings);
    }
  }

  @Override
  public boolean isEmpty() {
    return this.condition == null || this.condition.isEmpty();
  }

  @Override
  public void setColumn(final ColumnReference column) {
    if (this.condition != null) {
      this.condition.setColumn(column);
    }
  }

  public void setCondition(final Condition condition) {
    this.condition = condition;
  }

  @Override
  public boolean test(final MapEx record) {
    if (this.condition == null) {
      return true;
    } else {
      return this.condition.test(record);
    }
  }

  @Override
  public String toFormattedString() {
    if (this.condition == null) {
      return "";
    } else {
      return this.condition.toFormattedString();
    }
  }

  @Override
  public String toString() {
    if (this.condition == null) {
      return "";
    } else {
      return this.condition.toString();
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public <QV extends QueryValue> QV updateQueryValues(final TableReference oldTable,
    final TableReference newTable, final Function<QueryValue, QueryValue> valueHandler) {
    if (this.condition == null) {
      return (QV)this;
    } else {
      return this.condition.updateQueryValues(oldTable, newTable, valueHandler);
    }
  }

}
