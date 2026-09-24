package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.query.Column;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.ColumnWithPrefix;
import com.revolsys.record.query.From;
import com.revolsys.record.query.FromAlias;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class JsonbEach extends UnaryFunction implements From {

  public class Alias extends FromAlias {
    private final ColumnWithPrefix key;

    private final ColumnWithPrefix value;

    Alias(final String alias) {
      super(JsonbEach.this, alias);
      this.key = new ColumnWithPrefix(alias, new Column("key"));
      this.value = new ColumnWithPrefix(alias, new Column("value"));
    }

    public ColumnWithPrefix key() {
      return this.key;
    }

    public ColumnWithPrefix value() {
      return this.value;
    }
  }

  public static final String NAME = "jsonb_each";

  public JsonbEach(final QueryValue parameter) {
    super(NAME, parameter);
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
  public Object getValueFromResultSet(RecordDefinition recordDefinition, int fieldIndex,
    ResultSet resultSet, ColumnIndexes indexes, boolean internStrings) throws SQLException {
    throw new UnsupportedOperationException("jsonb_each can only be used in a from clause");
  }

  public QueryValue jsonField() {
    return getParameter();
  }

  @Override
  public Alias toFromAlias(final String alias) {
    return new Alias(alias);
  }

}
