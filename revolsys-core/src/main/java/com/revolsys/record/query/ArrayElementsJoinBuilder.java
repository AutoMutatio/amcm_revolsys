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

  public void aArrayFunction(final Function<QueryValue, ArrayElements> arrayFunction) {
    this.arrayFunction = arrayFunction;
  }

  public ArrayElementsJoinBuilder addVirtualField(final AbstractTableRecordStore recordStore,
    final String name) {
    return addVirtualField(recordStore, name, DataTypes.STRING);
  }

  /**
   * Add a new virtual field
   * @param recordStore The record store the add the field to.
   * @param name The name of the virtual field to add
   * @param dataType The field data type
   * @return self
   */
  public ArrayElementsJoinBuilder addVirtualField(final AbstractTableRecordStore recordStore, final String name,
    final DataType dataType) {
    recordStore.addVirtualField(name, dataType, true, (query, _, _, _) -> {
      getJoin(query);
      return new Column(this.joinAlias);
    });
    return this;
  }

  public String arrayFieldName() {
    return this.arrayFieldName;
  }

  public ArrayElementsJoinBuilder arrayFieldName(final String arrayFieldName) {
    ensureEditible();
    this.arrayFieldName = arrayFieldName;
    return this;
  }

  public Function<QueryValue, ArrayElements> arrayFunction() {
    return this.arrayFunction;
  }

  private void ensureEditible() {
    if (this.readonly) {
      throw new IllegalStateException("Readonly");
    }
  }

  public Join getJoin(final Query query) {
    final var arrayColumn = query.getColumn(this.arrayFieldName);
    var join = query.getJoin(this.joinAlias, j -> {
      final var statement = j.getStatement();
      if (statement instanceof final ArrayElements unnest) {
        if (unnest.getParameter()
          .equals(arrayColumn)) {
          return true;
        }
      }
      return false;
    });
    if (join == null) {
      join = query.join(JoinType.COMMA)
        .statement(this.arrayFunction.apply(arrayColumn))//
        .setAlias(this.joinAlias);
    }
    return join;
  }

  public String joinAlias() {
    return this.joinAlias;
  }

  public ArrayElementsJoinBuilder joinAlias(final String joinAlias) {
    ensureEditible();
    this.joinAlias = joinAlias;
    return this;
  }

  public ArrayElementsJoinBuilder makeReadonly() {
    this.readonly = true;
    return this;
  }

}
