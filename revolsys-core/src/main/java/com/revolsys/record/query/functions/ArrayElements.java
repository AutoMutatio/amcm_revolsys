package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.query.Column;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.From;
import com.revolsys.record.query.FromAlias;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.query.TableReferenceProxy;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class ArrayElements extends UnaryFunction implements From {

  public class Alias extends FromAlias {
    private final Column column;

    Alias(final String alias) {
      super(ArrayElements.this, alias);
      this.column = new Column(alias);
    }

    @Override
    public Column getColumn() {
      return this.column;
    }
  }

  public static ArrayElements jsonbArrayElements(final QueryValue parameter) {
    return new ArrayElements("jsonb_array_elements", parameter);
  }

  public static ArrayElements jsonbObjectKeys(final QueryValue parameter) {
    return new ArrayElements("jsonb_object_keys", parameter);
  }

  public static ArrayElements jsonbObjectKeys(final TableReferenceProxy table,
    final String fieldName) {
    final var column = table.getColumn(fieldName);
    return jsonbObjectKeys(column);
  }

  public static ArrayElements unnest(final QueryValue parameter) {
    return new ArrayElements("unnest", parameter);
  }

  private Alias alias;

  public ArrayElements(final String name, final QueryValue parameter) {
    super(name, parameter);
  }

  @Override
  public void appendFrom(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable string) {
    appendDefaultSql(statement, recordStore, string);
  }

  @Override
  public TableReference getTableReference() {
    return null;
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition, final int fieldIndex,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final QueryValue parameter = getParameter();
    return parameter.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes,
      internStrings);
  }

  @Override
  public Alias toFromAlias(final String alias) {
    if (this.alias == null) {
      this.alias = new Alias(alias);
      return this.alias;
    } else if (alias.equals(this.alias.getTableAlias())) {
      return this.alias;
    } else {
      return new Alias(alias);
    }
  }

}
