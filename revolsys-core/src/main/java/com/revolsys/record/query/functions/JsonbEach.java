package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.From;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class JsonbEach extends UnaryFunction implements From {

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
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    throw new UnsupportedOperationException("jsonb_each can only be used in a from clause");
  }

  public QueryValue jsonField() {
    return getParameter();
  }

}
