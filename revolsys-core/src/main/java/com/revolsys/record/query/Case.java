package com.revolsys.record.query;

import java.sql.PreparedStatement;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.schema.RecordStore;

public class Case implements QueryValue {

  public record When(Condition condition, QueryValue value) {
  }

  private final ListEx<When> whens = Lists.newArray();

  private QueryValue elseValue;

  public Case when(final Condition condition, final QueryValue value) {
    this.whens.add(new When(condition, value));
    return this;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append("CASE");

    this.whens.forEach(when -> {
      sql.append(" WHEN ");
      when.condition.appendDefaultSql(statement, recordStore, sql);
      sql.append(" THEN ");
      when.value.appendDefaultSql(statement, recordStore, sql);
    });

    if (this.elseValue != null) {
      sql.append(" ELSE ");
      this.elseValue.appendDefaultSql(statement, recordStore, sql);
    }
    sql.append(" END");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final var when : this.whens) {
      index = when.condition.appendParameters(index, statement);
      index = when.value.appendParameters(index, statement);
    }
    if (this.elseValue != null) {
      index = this.elseValue.appendParameters(index, statement);
    }
    return index;
  }

  @Override
  public QueryValue clone(final TableReference oldTable, final TableReference newTable) {
    final Case clone = new Case();
    this.whens.forEach(when -> {
      clone.when(when.condition.clone(oldTable, newTable), when.value.clone(oldTable, newTable));
    });
    if (this.elseValue != null) {
      clone.elseValue(this.elseValue.clone(oldTable, newTable));
    }
    return clone;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    for (final var when : this.whens) {
      if (when.condition.test(record)) {
        return when.value.getValue(record);
      }
    }
    if (this.elseValue != null) {
      return this.elseValue.getValue(record);
    } else {
      return null;
    }
  }

  public Case elseValue(final QueryValue elseValue) {
    this.elseValue = elseValue;
    return this;
  }
}
