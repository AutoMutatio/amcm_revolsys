package com.revolsys.record.query;

import java.math.BigDecimal;

import com.revolsys.collection.map.MapEx;
import com.revolsys.data.type.DataTypes;
import com.revolsys.record.schema.RecordStore;

public class Negate extends AbstractUnaryQueryValue {

  public Negate(final QueryValue value) {
    super(value);
  }

  @Override
  public void appendDefaultSql(final QueryStatement statement, final RecordStore recordStore,
    final SqlAppendable buffer) {
    buffer.append("- ");
    super.appendDefaultSql(statement, recordStore, buffer);
  }

  @Override
  public Negate clone() {
    return (Negate)super.clone();
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj instanceof Negate) {
      return super.equals(obj);
    }
    return false;
  }

  @Override
  public <V> V getValue(final MapEx record) {
    final Object value = getValue().getValue(record);
    if (value instanceof Number) {
      final BigDecimal number = DataTypes.DECIMAL.toObject(value);
      final BigDecimal result = number.negate();
      return DataTypes.toObject(value.getClass(), result);
    }
    return null;
  }

  @Override
  public String toString() {
    return "-" + super.toString();
  }
}
