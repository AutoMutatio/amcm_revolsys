package com.revolsys.record.query;

import java.util.function.Function;

import com.revolsys.data.type.DataType;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.query.functions.ArrayElements;
import com.revolsys.record.schema.AbstractTableRecordStore;

public class ArrayElementsJoinBuilder {

  private String arrayFieldName;

  private String joinAlias;

  private boolean readonly = false;

  public Function<QueryValue, ArrayElements> arrayFunction = ArrayElements::unnest;

  public void aArrayFunction(Function<QueryValue, ArrayElements> arrayFunction) {
    this.arrayFunction = arrayFunction;
  }

  public ArrayElementsJoinBuilder addVirtualField(AbstractTableRecordStore recordStore,
    String name) {
    return addVirtualField(recordStore, name, DataTypes.STRING);
  }

  /**
   * Add a new virtual field
   * @param recordStore The record store the add the field to.
   * @param name The name of the virtual field to add
   * @param dataType The field data type
   * @return self
   */
  public ArrayElementsJoinBuilder addVirtualField(AbstractTableRecordStore recordStore, String name,
    DataType dataType) {
    recordStore.addVirtualField(name, dataType, true, (query, _, _) -> {
      getJoin(query);
      return new Column(joinAlias);
    });
    return this;
  }

  public String arrayFieldName() {
    return arrayFieldName;
  }

  public ArrayElementsJoinBuilder arrayFieldName(String arrayFieldName) {
    ensureEditible();
    this.arrayFieldName = arrayFieldName;
    return this;
  }

  public Function<QueryValue, ArrayElements> arrayFunction() {
    return arrayFunction;
  }

  private void ensureEditible() {
    if (readonly) {
      throw new IllegalStateException("Readonly");
    }
  }

  public Join getJoin(final Query query) {
    final var arrayColumn = query.getColumn(arrayFieldName);
    var join = query.getJoin(joinAlias, j -> {
      final var statement = j.getStatement();
      if (statement instanceof final ArrayElements unnest) {
        if (unnest.getParameter().equals(arrayColumn)) {
          return true;
        }
      }
      return false;
    });
    if (join == null) {
      join = query.join(JoinType.COMMA)
        .statement(this.arrayFunction.apply(arrayColumn))//
        .setAlias(joinAlias);
    }
    return join;
  }

  public String joinAlias() {
    return joinAlias;
  }

  public ArrayElementsJoinBuilder joinAlias(String joinAlias) {
    ensureEditible();
    this.joinAlias = joinAlias;
    return this;
  }

  public ArrayElementsJoinBuilder makeReadonly() {
    this.readonly = true;
    return this;
  }

}
