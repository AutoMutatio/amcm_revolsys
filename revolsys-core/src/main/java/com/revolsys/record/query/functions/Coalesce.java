package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.RecordDefinition;

public class Coalesce extends SimpleFunction {
  public Coalesce() {
    super("COALESCE");
  }

  public Coalesce(final QueryValue... parameters) {
    super("COALESCE", parameters);
  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    int fieldIndex, final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    final QueryValue parameter = getParameter(0);
    return parameter.getValueFromResultSet(recordDefinition, fieldIndex, resultSet, indexes, internStrings);
  }
}
