package com.revolsys.record.query.functions;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.schema.RecordDefinition;

public class Max extends UnaryFunction {

  public static final String NAME = "MAX";

  public Max(final List<QueryValue> parameters) {
    super(NAME, parameters);
  }

  public Max(final QueryValue parameter) {
    super(NAME, parameter);
  }

  @Override
  public Object getValueFromResultSet(RecordDefinition recordDefinition, final ResultSet resultSet,
    final ColumnIndexes indexes, final boolean internStrings) throws SQLException {
    final QueryValue parameter = getParameter();
    return parameter.getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
  }

}