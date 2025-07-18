package com.revolsys.record.query.functions;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import com.revolsys.collection.json.Json;
import com.revolsys.collection.json.JsonObject;
import com.revolsys.collection.map.MapEx;
import com.revolsys.record.query.ColumnIndexes;
import com.revolsys.record.query.ColumnReference;
import com.revolsys.record.query.Condition;
import com.revolsys.record.query.Q;
import com.revolsys.record.query.QueryStatement;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.SqlAppendable;
import com.revolsys.record.query.StringLiteral;
import com.revolsys.record.query.StringValue;
import com.revolsys.record.query.Value;
import com.revolsys.record.schema.RecordDefinition;
import com.revolsys.record.schema.RecordStore;

public class JsonValue extends SimpleFunction {

  public static final String NAME = "JSON_VALUE";

  private String path;

  private String displayPath;

  private boolean text = true;

  public JsonValue(final List<QueryValue> parameters) {
    super(NAME, 2, parameters);
    final QueryValue pathParameter = parameters.get(1);
    if (pathParameter instanceof final StringValue value) {
      this.displayPath = value.getString();
    } else if (pathParameter instanceof final StringLiteral literal) {
      this.displayPath = literal.getString();
    } else if (Value.isString(pathParameter)) {
      this.displayPath = (String)((Value)pathParameter).getValue();
    } else {
      throw new IllegalArgumentException(
        "JSON_VALUE path parameter is not a string: " + pathParameter);
    }
    if (this.displayPath.matches("[\\s\\w]+(\\.[\\w\\s]+)*")) {
      this.path = "$." + this.displayPath;
    } else if (this.displayPath.matches("\\$(\\.[\\w\\s]+)*")) {
      this.path = this.displayPath;
    } else {
      throw new IllegalArgumentException(
        "JSON_VALUE path parameter must match $(.propertyName)* (e.g. $.address.city): "
          + pathParameter);
    }
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    final QueryValue jsonParameter = getParameter(0);

    buffer.append(getName());
    buffer.append("(");
    jsonParameter.appendSql(statement, recordStore, buffer);
    buffer.append(", '");
    buffer.append(this.path);
    buffer.append("')");
  }

  @Override
  public int appendParameters(int index, final PreparedStatement statement) {
    final QueryValue jsonParameter = getParameter(0);
    index = jsonParameter.appendParameters(index, statement);
    return index;
  }

  public Condition equal(final Object value) {
    final var column = getColumn();
    final var queryValue = Value.newValue(column, value);
    return Q.equal(this, queryValue);
  }

  @Override
  public ColumnReference getColumn() {
    return getQueryValues().get(0)
      .getColumn();
  }

  public String getPath() {
    return this.path;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <V> V getValue(final MapEx record) {

    final JsonObject value = getParameterValue(0, record, Json.JSON_OBJECT);
    final String path = getParameterStringValue(1, record);
    if (value != null) {
      final String[] names = path.split("\\.");
      final Object result = value.getByPath(names);
      if (result != null) {
        return (V)result.toString();
      }
    }
    return null;

  }

  @Override
  public Object getValueFromResultSet(final RecordDefinition recordDefinition,
    final ResultSet resultSet, final ColumnIndexes indexes, final boolean internStrings)
    throws SQLException {
    return getColumn().getValueFromResultSet(recordDefinition, resultSet, indexes, internStrings);
  }

  public boolean isText() {
    return this.text;
  }

  public JsonValue setText(final boolean text) {
    this.text = text;
    return this;
  }

  @Override
  public String toString() {
    final List<QueryValue> parameters = getParameters();
    return NAME + "(" + parameters.get(0) + ", '" + this.displayPath + "')";
  }
}
