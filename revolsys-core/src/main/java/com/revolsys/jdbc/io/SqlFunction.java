package com.revolsys.jdbc.io;

import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.StringBuilderSqlAppendable;

public class SqlFunction {
  private final String prefix;

  private String suffix = ")";

  public SqlFunction(final String functionName) {
    this.prefix = functionName + "(";
  }

  public SqlFunction(final String prefix, final String suffix) {
    this.prefix = prefix;
    this.suffix = suffix;
  }

  public String toSql(final Object... parameters) {
    final StringBuilderSqlAppendable sql = SqlAppendable.stringBuilder();
    sql.append(this.prefix);
    if (parameters.length > 0) {
      Object value = parameters[0];
      if (value == null) {
        sql.append("NULL");
      } else {
        sql.append(value);
      }
      for (int i = 1; i < parameters.length; i++) {
        sql.append(",");
        value = parameters[i];
        if (value == null) {
          sql.append("NULL");
        } else {
          sql.append(value);
        }
      }
    }
    sql.append(this.suffix);
    return sql.toSqlString();
  }
}
