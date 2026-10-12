package com.revolsys.record.query.functions;

import com.revolsys.collection.list.ListEx;
import com.revolsys.collection.list.Lists;
import com.revolsys.data.type.DataType;
import com.revolsys.exception.ExceptionWithProperties;
import com.revolsys.record.query.QueryValue;
import com.revolsys.record.query.Value;

public record FunctionDefinition(String name, DataType returnType,
  ListEx<DataType> parameterTypes) {

  public FunctionDefinition(final String name, final DataType returnType,
    final DataType... parameterTypes) {
    this(name, returnType, Lists.newArray(parameterTypes));
  }

  public FunctionMultiArgs instance(final Object... parameters) {
    if (parameters.length != this.parameterTypes.size()) {
      throw new ExceptionWithProperties(
        "There must be the same number of parameter types as values")
        .property("types", this.parameterTypes)
        .property("parameters", parameters);
    }
    final var params = Lists.newArray(parameters)
      .<QueryValue> mapIndex((i, value) -> {
        final var parameterType = this.parameterTypes.get(i);
        if (value instanceof final Value queryValue) {
          return queryValue.convertTo(parameterType);
        } else if (value instanceof final QueryValue queryValue) {
          return queryValue;
        } else {
          return Value.newValue(parameterType, value);
        }
      });
    return new FunctionMultiArgs(this.name, params);
  }
}
