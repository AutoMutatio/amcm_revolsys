package com.revolsys.record.query.functions;

import java.sql.PreparedStatement;
import java.util.List;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataType;
import com.revolsys.record.query.AbstractMultiQueryValue;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.StringBuilderSqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordStore;

public class FunctionMultiArgs extends AbstractMultiQueryValue implements Function {

  private final String name;

  public FunctionMultiArgs(final String name, final Iterable<? extends QueryValue> arguments) {
    super(arguments);
    this.name = name;
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable sql) {
    sql.append(this.name);
    sql.append("(");
    boolean first = true;

    for (final QueryValue value : this.values) {
      if (first) {
        first = false;
      } else {
        sql.append(',');
      }
      value.appendSelect(statement, recordStore, sql);
    }
    sql.append(")");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    for (final QueryValue value : this.values) {
      if (value != null) {
        index = value.appendParameters(index, statement);
      }
    }
    return index;
  }

  @Override
  public FunctionMultiArgs clone() {
    return (FunctionMultiArgs)super.clone();
  }

  @Override
  public FunctionMultiArgs clone(final TableReference oldTable, final TableReference newTable) {
    return (FunctionMultiArgs)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof final FunctionMultiArgs function) {
      if (DataType.equal(getName(), function.getName())) {
        return super.equals(function);
      }
    }
    return false;
  }

  @Override
  public String getName() {
    return this.name;
  }

  @Override
  public int getParameterCount() {
    return getQueryValues().size();
  }

  @Override
  public List<QueryValue> getParameters() {
    return getQueryValues();
  }

  @Override
  public <V> V getValue(final MapEx record) {
    throw new UnsupportedOperationException("getValue");
  }

  @Override
  public String toString() {
    final var sql = new StringBuilderSqlAppendable();
    appendDefaultSelect(null, null, sql);
    return sql.toString();
  }
}
