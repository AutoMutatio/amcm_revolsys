package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.From;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.TableReference;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class Unnest extends UnaryFunction implements From {

  public static final String NAME = "unnest";

  public Unnest(final List<QueryValue> parameters) {
    super(NAME, parameters);
  }

  public Unnest(final QueryValue parameter) {
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
    final QueryValue parameter = getParameter();
    return parameter.getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
  }

}
